import logging
import re
import csv
import time
import random
import os
import requests
from urllib.parse import urlparse, quote_plus, parse_qs
from bs4 import BeautifulSoup
from typing import List, Optional, Tuple, Set, Any
import openai

# Selenium imports matching legacy usage
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
#from ddg_search_improved import ddg_search_improved as ddg_search_snippets

# Local imports
from ..config.settings import OPENAI_API_KEY
from ..utils.text_utils import clean_url, normalize_text

logger = logging.getLogger(__name__)

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

def find_official_site_via_web(company_name: str, comune: str) -> str:
    """Esegue una web search leggera per trovare il sito ufficiale di un'azienda."""
    try:
        query = f"{company_name} {comune} sito ufficiale"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }
        
        def is_bad(domain: str) -> bool:
            d = domain.lower()
            bad = [
                'google.', 'gstatic.com', 'googleusercontent.com', 'maps.googleapis.', 'support.google.', 'policies.google.',
                'facebook.com', 'instagram.com', 'linkedin.com', 'paginegialle', 'tripadvisor', 'youtube.com', 'tiktok.com',
                'amazon.', 'ebay.', 'subito.', 'wikipedia.org'
            ]
            return any(b in d for b in bad)

        # 1) Google SERP
        url_g = f"https://www.google.com/search?q={quote_plus(query)}&hl=it"
        r = requests.get(url_g, headers=headers, timeout=10)
        candidates = []
        if r.status_code == 200 and r.text:
            soup = BeautifulSoup(r.text, 'html.parser')
            seen = set()
            for a in soup.select('div.yuRUbf > a[href], div.g a[href], #search a[href]'):
                href = a.get('href') or ''
                if href.startswith('/url?'):
                    try:
                        qs = parse_qs(urlparse(href).query)
                        href = qs.get('q', [''])[0]
                    except:
                        continue
                if not href.startswith('http'):
                    continue
                netloc = urlparse(href).netloc.lower().replace('www.', '')
                if is_bad(netloc):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                candidates.append(href)
                if len(candidates) >= 5:
                    break

        # 2) DuckDuckGo HTML fallback
        if not candidates:
            url_ddg = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
            r2 = requests.get(url_ddg, headers=headers, timeout=10)
            if r2.status_code == 200 and r2.text:
                soup2 = BeautifulSoup(r2.text, 'html.parser')
                for a in soup2.select('a.result__a[href]'):
                    href = a.get('href') or ''
                    if not href.startswith('http'):
                        continue
                    netloc = urlparse(href).netloc.lower().replace('www.', '')
                    if is_bad(netloc):
                        continue
                    candidates.append(href)
                    if len(candidates) >= 5:
                        break

        if candidates:
            first = candidates[0]
            parsed = urlparse(first)
            if parsed.scheme and parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"

        try:
            from ..utils.text_utils import clean_url
            base = re.sub(r'[^a-z0-9]+', '', (company_name or '').lower())
            guess = f"https://www.{base}.it"
            g = requests.head(guess, headers=headers, timeout=5, allow_redirects=True)
            if 200 <= g.status_code < 400:
                return clean_url(guess)
        except:
            pass
        return ""
    except Exception as e:
        logger.debug(f"find_official_site_via_web error: {e}")
        return ""

def _same_domain(url: str, candidate: str) -> bool:
    try:
        u = urlparse(url)
        c = urlparse(candidate)
        return (u.netloc.split(':')[0].lower().replace('www.', '') == c.netloc.split(':')[0].lower().replace('www.', ''))
    except:
        return False

def clean_extracted_text(text):
    """Pulisce il testo estratto rimuovendo prefissi, caratteri indesiderati e spazi iniziali"""
    if not text:
        return ""
    
    # Rimuovi prefissi comuni
    prefixes = ["Indirizzo:", "Address:", "Telefono:", "Phone:", "Tel:", "Website:", "Sito web:"]
    cleaned = text
    for prefix in prefixes:
        cleaned = cleaned.replace(prefix, "")
    
    # Rimuovi caratteri di controllo e spazi extra
    cleaned = re.sub(r'[\n\r\t]', ' ', cleaned)  # Sostituisci newline, tab ecc. con spazi
    cleaned = re.sub(r'\s+', ' ', cleaned)       # Riduci spazi multipli a uno solo
    
    # Rimuovi caratteri speciali e spazi all'inizio
    cleaned = re.sub(r'^[\s,.:;-]+', '', cleaned)
    
    return cleaned.strip()

def find_contact_subpages(base_url):
    """
    Scopre rapidamente sotto-pagine contatto usando:
    1) robots.txt -> sitemap seed
    2) fallback sitemap endpoints
    3) BFS su sitemapindex/urlset (con priorità alle sitemap di pagine)
    4) homepage links
    5) slug comuni (solo se servono)

    Ritorna lista ordinata di URL candidati.
    """
    if not base_url:
        return []

    import re
    import time
    from urllib.parse import urlparse, urljoin, urlunparse
    from xml.etree import ElementTree as ET

    CONTACT_KEYS = (
        "contact", "contacts", "contact-us", "contactus",
        "contatto", "contatti", "contattaci",
        "chi-siamo", "chisiamo", "about",
        "azienda", "company", "dove-siamo", "dovesiamo", "assistenza"
    )

    DEFAULT_SITEMAP_PATHS = (
        "/sitemap.xml",
        "/sitemap_index.xml",
        "/sitemap-index.xml",
        "/wp-sitemap.xml",
        "/sitemap.php",
    )

    # Evita di buttare dentro asset / file non-HTML come candidati
    SKIP_EXT = (
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
        ".pdf", ".zip", ".rar", ".7z",
        ".mp4", ".mov", ".avi", ".mp3", ".wav",
        ".css", ".js",
        ".json", ".xml"  # sitemap/xml non è una pagina contatti
    )

    MAX_SITEMAPS_TO_FETCH = 30
    MAX_URLS_TO_SCAN = 20000

    # Se arrivo a X candidati "forti", posso evitare di aggiungere fallback slug
    STRONG_CANDIDATES_STOP = 8

    # ---------------- helpers ----------------
    def _strip_ns(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower() if "}" in tag else tag.lower()

    def _ensure_scheme(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return u
        if not u.startswith(("http://", "https://")):
            return "https://" + u.lstrip("/")
        return u

    def _base_root(u: str) -> str:
        """Sempre scheme://netloc/ (no path), come nello standalone."""
        u = _ensure_scheme(u)
        p = urlparse(u)
        if not p.netloc:
            host = u.split("/")[0]
            return f"https://{host}/"
        scheme = p.scheme or "https"
        return f"{scheme}://{p.netloc}/"

    def _canonicalize_url(u: str) -> str:
        """
        Normalizza URL per dedup:
        - toglie fragment
        - mantiene query (può essere utile raramente, ma per contatti quasi mai)
        - rimuove slash finale tranne root
        """
        try:
            p = urlparse(u)
            # rimuovi fragment
            p = p._replace(fragment="")
            # normalizza path slash
            path = p.path or "/"
            if path != "/" and path.endswith("/"):
                path = path[:-1]
            p = p._replace(path=path)
            return urlunparse(p)
        except Exception:
            return u

    def _is_probably_page(u: str) -> bool:
        if not u:
            return False
        ul = u.lower().split("?", 1)[0].split("#", 1)[0]
        return not ul.endswith(SKIP_EXT)

    def _safe_get(session, url, timeout=(6.0, 12.0), retries=2, allow_redirects=True):
        last = None
        for attempt in range(retries + 1):
            try:
                return session.get(url, timeout=timeout, allow_redirects=allow_redirects)
            except Exception as e:
                last = e
                time.sleep(0.6 * (attempt + 1))
        logger.debug(f"[http] GET failed url={url} err={last}")
        return None

    def _extract_sitemap_urls_from_robots(robots_text: str):
        urls = []
        for line in robots_text.splitlines():
            s = line.strip()
            if s.lower().startswith("sitemap:"):
                u = s.split(":", 1)[1].strip()
                if u:
                    urls.append(u)
        return list(dict.fromkeys(urls))

    def _parse_sitemap_xml(text: str):
        """
        Ritorna (child_sitemaps, urlset_locs)
        - sitemapindex -> child_sitemaps
        - urlset -> urlset_locs
        """
        if not text:
            return [], []
        try:
            root = ET.fromstring(text.strip().encode("utf-8", errors="ignore"))
            root_name = _strip_ns(root.tag)

            locs = []
            for el in root.iter():
                if _strip_ns(el.tag) == "loc" and el.text:
                    locs.append(el.text.strip())

            if root_name == "sitemapindex":
                return locs, []
            if root_name == "urlset":
                return [], locs

            return [], locs
        except Exception:
            # fallback regex su XML "sporco"
            locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", text, flags=re.IGNORECASE)
            return [], locs

    def _sitemap_priority(u: str) -> int:
        """
        Priorità per ridurre spreco:
        0 = pagine (contatti/chi-siamo stanno quasi sempre qui)
        1 = post
        2 = altro (taxonomy, product, author, ecc.)
        """
        ul = (u or "").lower()
        if "page-sitemap" in ul or "pages-sitemap" in ul or "wp-sitemap-posts-page" in ul:
            return 0
        if "post-sitemap" in ul or "wp-sitemap-posts-post" in ul:
            return 1
        return 2

    # ---------------- main ----------------
    try:
        base_clean = clean_url(base_url)  # manteniamo la tua funzione
        base = _base_root(base_clean)

        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xml,text/xml;q=0.9,*/*;q=0.8",
        })

        logger.debug(f"[base] input={base_url!r} clean={base_clean!r} root={base!r}")

        candidates = []  # (score, url)

        # 1) robots -> sitemap seeds
        sitemap_seeds = []
        robots_url = urljoin(base, "robots.txt")

        logger.info(f"[robots] GET {robots_url}")
        r = _safe_get(session, robots_url, timeout=(6.0, 12.0), retries=2, allow_redirects=True)

        # Se robots redirige (es. http->https o no-www->www), fissa base al final_url
        if r is not None and getattr(r, "url", None):
            base = _base_root(r.url)

        if r is not None:
            logger.info(f"[robots] status={r.status_code} final_url={r.url}")
            if getattr(r, "text", None):
                # se vuoi anche la debug line come nello standalone, metti debug qui
                # logger.debug(f"[robots] first_300_chars={r.text[:300]!r}")
                pass

            if r.status_code == 200 and r.text:
                sitemap_seeds = _extract_sitemap_urls_from_robots(r.text)
                logger.info(f"[robots] Sitemap lines found: {len(sitemap_seeds)}")
            else:
                logger.info("[robots] Nessuna sitemap trovata in robots.txt")
        else:
            logger.info("[robots] Errore/timeout su robots.txt")

        # 1b) fallback sitemap endpoints se robots non ne ha
        if not sitemap_seeds:
            sitemap_seeds = [urljoin(base, p.lstrip("/")) for p in DEFAULT_SITEMAP_PATHS]

        # BFS su sitemaps (sitemapindex/urlset)
        fetched = set()
        queue = list(dict.fromkeys([u for u in sitemap_seeds if isinstance(u, str) and u.startswith(("http://", "https://"))]))

        urls_scanned = 0
        sitemap_candidates = []

        while queue and len(fetched) < MAX_SITEMAPS_TO_FETCH and urls_scanned < MAX_URLS_TO_SCAN:
            sm_url = queue.pop(0)
            if sm_url in fetched:
                continue
            fetched.add(sm_url)

            logger.info(f"[sitemap] GET {sm_url}")
            sm = _safe_get(session, sm_url, timeout=(6.0, 16.0), retries=2, allow_redirects=True)
            if sm is None:
                continue

            logger.info(f"[sitemap] status={sm.status_code} final_url={sm.url} content_type={sm.headers.get('Content-Type')}")
            if sm.status_code != 200 or not sm.text:
                continue

            child_sitemaps, url_locs = _parse_sitemap_xml(sm.text)

            # sitemapindex => enqueue children (priorità pagine)
            if child_sitemaps:
                child_sitemaps = [c for c in child_sitemaps if c and c.startswith(("http://", "https://"))]
                child_sitemaps = sorted(child_sitemaps, key=_sitemap_priority)
                for child in child_sitemaps:
                    if child not in fetched:
                        queue.append(child)
                continue

            # urlset => filtra url
            if url_locs:
                for loc in url_locs:
                    urls_scanned += 1
                    if urls_scanned >= MAX_URLS_TO_SCAN:
                        break

                    if not loc.startswith(("http://", "https://")):
                        continue
                    if not _same_domain(base, loc):
                        continue
                    if not _is_probably_page(loc):
                        continue

                    loc_l = loc.lower()
                    if any(k in loc_l for k in CONTACT_KEYS):
                        sitemap_candidates.append(loc)

                # early-stop: se già ho abbastanza candidati sitemap utili
                if len(set(sitemap_candidates)) >= 20:
                    break

        # aggiungi sitemap candidates (score forte 4)
        for u in dict.fromkeys(sitemap_candidates):
            candidates.append((4, u))

        # 2) Homepage parsing (con filtro asset)
        try:
            logger.info(f"Parsing homepage for links: {base}")
            hp = _safe_get(session, base, timeout=(6.0, 16.0), retries=1, allow_redirects=True)

            # Se homepage redirige ulteriormente, fissa base al final_url
            if hp is not None and getattr(hp, "url", None):
                base = _base_root(hp.url)

            if hp is not None and hp.status_code == 200:
                soup = BeautifulSoup(hp.text, "html.parser")
                anchors = soup.find_all("a", href=True)

                for a in anchors:
                    href = a["href"].strip()
                    text = (a.get_text() or "").strip().lower()

                    # Normalizza href -> assoluto
                    if href.startswith("/"):
                        abs_url = urljoin(base, href)
                    elif href.startswith(("http://", "https://")):
                        abs_url = href
                    else:
                        continue

                    if not _same_domain(base, abs_url):
                        continue
                    if not _is_probably_page(abs_url):
                        continue

                    href_l = abs_url.lower()
                    score = 0

                    if any(k in href_l for k in ["contatto", "contatti", "contattaci", "contact", "contact-us", "contacts", "contactus"]):
                        score += 3
                    if any(k in text for k in ["contatti", "contatto", "contattaci", "contact"]):
                        score += 2
                    if any(k in href_l for k in ["chi-siamo", "chisiamo", "about", "azienda", "company"]):
                        score += 1

                    if score > 0:
                        candidates.append((score, abs_url))

            elif hp is not None:
                logger.warning(f"Homepage returned status {hp.status_code}")
        except Exception as e:
            logger.warning(f"Errore parsing homepage: {e}")

        # 3) Slug comuni fallback (solo se non ho già abbastanza candidati “buoni”)
        strong_count = sum(1 for s, _ in candidates if s >= 3)
        if strong_count < STRONG_CANDIDATES_STOP:
            common = [
                "/contatti", "/contatti/", "/contatto", "/contattaci",
                "/contact", "/contacts", "/contact-us", "/contactus",
                "/chi-siamo", "/about", "/azienda", "/company",
                "/dove-siamo", "/assistenza",
            ]
            for slug in common:
                candidates.append((1, urljoin(base, slug.lstrip("/"))))

        # 4) Dedup + sort (dedup canonical)
        seen = set()
        scored = []
        for score, link in candidates:
            if not link:
                continue

            canon = _canonicalize_url(link)
            if canon in seen:
                continue

            # filtra di nuovo per sicurezza
            if not _is_probably_page(canon):
                continue

            seen.add(canon)
            scored.append((score, canon))

        scored.sort(key=lambda x: (-x[0], len(x[1])))
        return [link for _, link in scored]

    except Exception as e:
        logger.debug(f"Errore in find_contact_subpages: {e}")
        return []




def extract_emails_from_website(url, disable_slug_fallback=False):
    """Estrae email da un sito web concentrandosi sulle aree più probabili e filtrando i risultati sporchi."""
    if not url or not url.startswith(("http://", "https://")):
        return []
    
    # Estrai il nome del dominio per il debug
    try:
        domain = urlparse(url).netloc.replace('www.', '')
    except:
        domain = "unknown"
    
    # Domini da ignorare (provider generici, servizi temporanei, blacklist)
    IGNORED_EMAIL_DOMAINS = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "live.com", "msn.com", "icloud.com",
        "sentry.io", "wixpress.com", "example.com", "test.com", "yourdomain.com", "mydomain.com", "website.com", "domain.com", "localhost",
        "google.com", "facebook.com", "twitter.com", "instagram.com", "doubleclick.net", "amazonaws.com", "appspot.com", "cdn.com", "cloudfront.net",
        "windows.net", "azure.com", "microsoft.com", "apple.com", "broofa.com"
    }
    
    # Pattern per local part sospetti
    LOCAL_PART_IGNORE_PATTERNS = [
        re.compile(r"^[a-f0-9]{24,}$"),  # hash
        re.compile(r"^[a-z0-9]{30,}$"),
        re.compile(r"^(noreply|no-reply|donotreply|unsubscribe|mailer-daemon|postmaster|abuse|bounces?|devnull|null)$", re.I),
        re.compile(r"privacy|gdpr|legal|copyright", re.I),
        re.compile(r"^.{1,2}@"),
    ]
    
    emails_footer = set()
    emails_contact = set()
    emails_mailto = set()
    emails_page = set()
    all_emails = set()
    logger.info(f"\n{'='*50}\nInizio estrazione email da: {url}\n{'='*50}")
    
    try:
        # Individua rapidamente sotto-pagine di contatto a partire dall'homepage
        paths_to_check = [""]
        discovery_links = []
        try:
            discovery_links = find_contact_subpages(url)
        except Exception as e:
            logger.warning(f"find_contact_subpages KO per {url}: {e}", exc_info=True)
            discovery_links = []
        # Aggiungi solo i link scoperti
        for link in discovery_links:
            try:
                if link.startswith('http') and _same_domain(url, link):
                    # Conserva l'URL assoluto così com'è
                    paths_to_check.append(link)
            except:
                continue
        # Se discovery non ha prodotto nulla e non è disabilitato il fallback, aggiungi slug comuni
        if not discovery_links and not disable_slug_fallback:
            paths_to_check.extend([
                "/contatti", "/contatto", "/contattaci", "/contattaci/", "/contatti/",
                "/contact", "/contacts", "/contact-us", "/contactus",
                "/chi-siamo", "/chisiamo", "/about", "/about-us", "/azienda", "/company",
                "/privacy", "/legal"
            ])
            
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})

        def normalize_obfuscations(text):
            if not text:
                return ""
            t = text
            replacements = [
                (r"\s*\[at\]\s*", "@"), (r"\s*\(at\)\s*", "@"), (r"\s+at\s+", "@"), (r"\s*\[chiocciola\]\s*", "@"),
                (r"\s*\[dot\]\s*", "."), (r"\s*\(dot\)\s*", "."), (r"\s+dot\s+", "."), (r"\s*punto\s*", "."),
                (r"\s*\[punto\]\s*", "."), (r"\s*\(punto\)\s*", ".")
            ]
            for pattern, repl in replacements:
                try:
                    t = re.sub(pattern, repl, t, flags=re.IGNORECASE)
                except:
                    continue
            t = re.sub(r"\s*@\s*", "@", t)
            t = re.sub(r"\s*\.\s*", ".", t)
            return t

        for path in paths_to_check:
            try:
                # Normalizza path/URL: se è assoluto usalo, altrimenti risolvi rispetto a url
                try:
                    from urllib.parse import urljoin
                except Exception:
                    urljoin = None
                if path.startswith('http'):
                    full_url = path
                else:
                    full_url = urljoin(url, path) if urljoin else (url.rstrip("/") + path)
                logger.info(f"\nAnalisi pagina: {full_url}")
                response = None
                for attempt in range(3):
                    try:
                        response = session.get(full_url, timeout=12)
                        if response.status_code == 200 and response.text:
                            break
                    except Exception as e:
                        if attempt == 2:
                            pass # Ignora
                        time.sleep(0.8 * (attempt + 1))
                if response and response.status_code == 200:
                    content = response.text
                    content_norm = normalize_obfuscations(content)
                    soup = BeautifulSoup(content_norm, 'html.parser')
                    
                    def clean_email(email):
                        """Pulisce e normalizza l'email rimuovendo testo aggiuntivo e parametri."""
                        # Rimuovi tutto dopo il primo punto interrogativo o spazio
                        email = re.split(r'[?\s]', email)[0]
                        
                        # Rimuovi testo aggiuntivo dopo il dominio
                        email = re.sub(r'@[^@]+?(?=\s|$)', lambda m: m.group(0).split()[0], email)
                        
                        # Rimuovi caratteri non validi
                        email = re.sub(r'[^\w.@-]', '', email)
                        # Rimuovi punteggiatura residua finale
                        email = email.rstrip('.,;:)')
                        
                        # Rimuovi prefissi comuni
                        for prefix in ['mailto:', 'email', 'e-mail', 'Email', 'E-mail']:
                            if email.lower().startswith(prefix.lower()):
                                email = email[len(prefix):].strip()
                        
                        # Normalizza a lowercase
                        email = email.lower().strip()
                        
                        # Rimuovi duplicati nel dominio (es. example.com.com)
                        if '@' in email:
                            local, domain = email.split('@')
                            domain_parts = domain.split('.')
                            if len(domain_parts) > 2 and domain_parts[-1] == domain_parts[-2]:
                                domain = '.'.join(domain_parts[:-1])
                            email = f"{local}@{domain}"
                        
                        return email
                    
                    # 1. Footer (usa get_text e anche inner HTML per casi con simboli non testuali)
                    footer = soup.find('footer')
                    if footer:
                        footer_text = footer.get_text(" ") + " " + (footer.decode() if hasattr(footer, 'decode') else str(footer))
                        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', footer_text)
                        for email in emails:
                            cleaned = clean_email(email)
                            if cleaned:
                                emails_footer.add(cleaned)
                    
                    # 2. Sezione contatti
                    contact_sections = soup.find_all(['div', 'section'], class_=lambda x: x and ('contact' in x.lower() or 'contatti' in x.lower()))
                    for section in contact_sections:
                        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', section.get_text())
                        for email in emails:
                            cleaned = clean_email(email)
                            if cleaned:
                                emails_contact.add(cleaned)
                    
                    # 3. Link mailto
                    mailto_links = soup.find_all('a', href=lambda x: x and x.startswith('mailto:'))
                    for link in mailto_links:
                        email = link['href'].replace('mailto:', '').strip()
                        email = email.split('?')[0].strip()
                        if '@' in email:
                            cleaned = clean_email(email)
                            if cleaned:
                                emails_mailto.add(cleaned)
                    
                    # 4. Tutto il testo (anche HTML completo per simboli @ inseriti via CSS/JS)
                    page_text = soup.get_text(" ") + " " + (soup.decode() if hasattr(soup, 'decode') else content_norm)
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', page_text)
                    for email in emails:
                        cleaned = clean_email(email)
                        if cleaned:
                            emails_page.add(cleaned)

                    # 5. Data attributes e JSON incorporati
                    html_text = soup.decode() if hasattr(soup, 'decode') else content_norm
                    for attr_match in re.findall(r'data-email\s*=\s*"([^"]+)"', html_text, flags=re.IGNORECASE):
                        candidate = clean_email(normalize_obfuscations(attr_match))
                        if '@' in candidate:
                            emails_page.add(candidate)
                    for json_email in re.findall(r'"email"\s*:\s*"([^"]+)"', html_text, flags=re.IGNORECASE):
                        candidate = clean_email(normalize_obfuscations(json_email))
                        if '@' in candidate:
                            emails_page.add(candidate)
                    
                    # LinkedIn
                    linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[a-zA-Z0-9%_-]+/?'
                    linkedin_links = re.findall(linkedin_pattern, page_text)
                    for link in linkedin_links:
                        all_emails.add("LINKEDIN:" + link.strip())
            
            except Exception as e:
                logger.warning(f"Errore durante l'analisi di {full_url}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Errore generale nell'estrazione delle email da {url}: {str(e)}")
    
    # Unisci tutte le email trovate
    all_emails.update(emails_footer)
    all_emails.update(emails_contact)
    all_emails.update(emails_mailto)
    all_emails.update(emails_page)
    
    # Filtro avanzato
    def is_valid(email):
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
        local, domain = email.split('@', 1)
        if domain in IGNORED_EMAIL_DOMAINS:
            return False
        for pat in LOCAL_PART_IGNORE_PATTERNS:
            if pat.search(local):
                return False
        if len(email) > 254:
            return False
        if len(local) < 2:
            return False
        # Filtra email di Robert Kieffer che appaiono in alcuni snippet/librerie
        if "robert@broofa.com" in email:
            return False
        return True
    
    # Ordina: prima footer, poi contatti, poi mailto, poi testo
    ordered = list(emails_footer) + list(emails_contact) + list(emails_mailto) + list(emails_page)
    
    # Tieni solo email valide e uniche, preferendo email aziendali
    result = []
    seen = set()
    for email in ordered:
        if email not in seen and is_valid(email):
            seen.add(email)
            result.append(email)
    
    # Se ci sono email aziendali (non provider generici), tieni solo quelle
    aziendali = [e for e in result if not any(e.endswith('@' + d) for d in [
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "live.com", "msn.com", "icloud.com"
    ])]
    if aziendali:
        result = aziendali
    
    # Aggiungi LinkedIn
    for item in all_emails:
        if item.startswith("LINKEDIN:"):
            result.append(item)
    
    logger.info(f"\n{'='*50}\nEmail finali trovate: {result}\n{'='*50}\n")
    
    # Debug: salva le email trovate in un CSV
    debug_file = "debug_emails.csv"
    try:
        # Crea il file se non esiste
        if not os.path.exists(debug_file):
            with open(debug_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Dominio', 'Email Trovate', 'URL'])
        
        # Aggiungi le nuove email
        with open(debug_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([domain, '; '.join(result), url])
            
    except Exception as e:
        logger.error(f"Errore nel salvataggio del debug CSV: {str(e)}")
    
    return result

def extract_emails_with_driver(url, driver):
    """Fallback con Selenium per estrarre email quando le richieste HTTP falliscono o non trovano nulla.
    (Previously _extract_emails_with_driver in legacy)"""
    try:
        if not driver or not url:
            return []
        base = clean_url(url)
        paths_to_check = [
            "",
            "/contatti", "/contatto", "/contattaci", "/contattaci/", "/contatti/",
            "/contact", "/contacts", "/contact-us", "/contactus",
            "/chi-siamo", "/chisiamo", "/about", "/about-us", "/azienda", "/company",
        ]
        found = set()
        for path in paths_to_check:
            try:
                full_url = base.rstrip("/") + path
                logger.info(f"[Selenium Email] Carico: {full_url}")
                driver.get(full_url)
                time.sleep(2.0)
                html = driver.page_source or ""
                # Normalizza alcune offuscazioni comuni
                html = re.sub(r"\s*\[at\]\s*|\s*\(at\)\s*|\sat\s", "@", html, flags=re.IGNORECASE)
                html = re.sub(r"\s*\[dot\]\s*|\s*\(dot\)\s*|\spunto\s|\sdot\s", ".", html, flags=re.IGNORECASE)
                emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)
                for e in emails:
                    e = e.strip().lower()
                    if e:
                        found.add(e)
                # mailto
                mailtos = re.findall(r"mailto:([^'\" >?#]+)", html, flags=re.IGNORECASE)
                for e in mailtos:
                    e = e.split('?')[0].strip().lower()
                    if '@' in e:
                        found.add(e)
            except Exception as e:
                logger.debug(f"[Selenium Email] Errore su {full_url}: {e}")
                continue
        return list(found)
    except Exception as e:
        logger.debug(f"[Selenium Email] Errore generale: {e}")
        return []

def search_contact_info(company_name, log_file="log_google_snippet.csv"): 
    """Cerca informazioni sul contatto usando Google Search, estraendo titoli e snippet/metadescrizioni da più selettori e loggando i risultati."""
    logger.info(f"Inizio ricerca contatti per: {company_name}")
    try:
        log_path = os.path.abspath(log_file)
    except:
        log_path = log_file
    logger.info(f"File di log impostato su: {log_path}")
    
    try:
        # Query più variegate per aumentare le possibilità di trovare risultati
        search_queries = [
            f"{company_name} amministratore",
            f"{company_name} amministratore delegato",
            f"{company_name} proprietario",
            f"{company_name} titolare",
            f"{company_name} direttore",
            f"{company_name} responsabile",
            f"{company_name} imprenditore",
            f"{company_name} fondatore",
            # Aggiungiamo varianti più generiche
            f"{company_name} chi siamo",
            f"{company_name} contatti",
            f"{company_name} chi è",
            f"{company_name} chi sono",
            # Aggiungiamo query specifiche per ingegneri/tecnici
            f"{company_name} ingegnere",
            f"{company_name} tecnico",
            f"{company_name} progettista"
        ]
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        }
        
        search_results = []
        log_rows = []
        total_queries = len(search_queries)
        successful_queries = 0
        
        for i, query in enumerate(search_queries, 1):
            try:
                logger.info(f"Esecuzione query {i}/{total_queries}: {query}")
                encoded_query = quote_plus(query)
                url = f"https://www.google.com/search?q={encoded_query}&hl=it"
                
                # Aggiungiamo un delay variabile tra le query
                if i > 1:
                    delay = random.uniform(1, 2)
                    logger.info(f"Attesa di {delay:.2f} secondi prima della prossima query...")
                    time.sleep(delay)
                
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    # Verifica che la risposta contenga effettivamente HTML
                    if not response.text or len(response.text) < 1000:
                        logger.error(f"Risposta HTML troppo corta o vuota per la query: {query}")
                        logger.debug(f"Lunghezza risposta: {len(response.text)} caratteri")
                        continue
                    
                    # Verifica che la risposta contenga elementi chiave di Google
                    if "google.com" not in response.text and "search" not in response.text.lower():
                        logger.error(f"La risposta non sembra essere una pagina di Google per la query: {query}")
                        continue
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    results_found = 0
                    
                    # Log del contenuto della risposta per debug
                    logger.debug(f"Lunghezza risposta HTML: {len(response.text)} caratteri")
                    
                    # Verifica se ci sono risultati
                    no_results = soup.find("div", {"id": "result-stats"})
                    if no_results and "Nessun risultato" in no_results.text:
                        logger.warning(f"Nessun risultato trovato per la query: {query}")
                        continue
                    
                    # Verifica la presenza di risultati
                    search_results_div = soup.find("div", {"id": "search"})
                    if not search_results_div:
                        logger.error(f"Elemento #search non trovato nella pagina per la query: {query}")
                        continue
                    
                    # Verifica la struttura dei risultati
                    result_divs = soup.find_all("div", class_="g")
                    if not result_divs:
                        logger.error(f"Nessun div con classe 'g' trovato nella pagina per la query: {query}")
                        # Prova a salvare l'HTML per debug
                        debug_file_html = f"debug_google_response_{i}.html"
                        with open(debug_file_html, "w", encoding="utf-8") as f:
                            f.write(response.text)
                        logger.info(f"HTML salvato in {debug_file_html} per debug")
                        continue
                    
                    logger.info(f"Trovati {len(result_divs)} div risultati nella pagina")
                    
                    # Aumentato il numero di risultati da analizzare a 20
                    for result in result_divs[:20]:
                        title = result.find("h3")
                        if not title:
                            logger.debug("Titolo non trovato in un risultato")
                            continue
                            
                        snippet = None
                        snippet_found = False
                        
                        # Manteniamo tutti i selettori per maggiore affidabilità
                        for snippet_selector in [
                            ("div", "VwiC3b"),  # snippet classico
                            ("span", "aCOpRe"), # vecchio Google
                            ("div", "IsZvec"),  # nuovo Google
                            ("div", "st")        # ancora vecchio
                        ]:
                            tag, cls = snippet_selector
                            snippet_elem = result.find(tag, class_=cls)
                            if snippet_elem:
                                snippet = snippet_elem.get_text()
                                snippet_found = True
                                logger.debug(f"Snippet trovato con selettore: {tag}.{cls}")
                                break
                        
                        if not snippet:
                            span = result.find("span")
                            if span:
                                snippet = span.get_text()
                                snippet_found = True
                                logger.debug("Snippet trovato in span generico")
                        
                        if not snippet_found:
                            logger.debug("Nessuno snippet trovato per questo risultato")
                            continue
                        
                        link = result.find("a")
                        link_url = link.get("href") if link else ""
                        
                        if not link_url:
                            logger.debug("URL non trovato per questo risultato")
                            continue
                        
                        if title and snippet:
                            search_results.append({
                                "title": title.get_text(),
                                "snippet": snippet,
                                "url": link_url,
                                "query": query
                            })
                            log_rows.append({
                                "azienda": company_name,
                                "query": query,
                                "title": title.get_text(),
                                "snippet": snippet,
                                "url": link_url
                            })
                            results_found += 1
                            logger.debug(f"Risultato {results_found} aggiunto: {title.get_text()[:50]}...")
                    
                    if results_found > 0:
                        successful_queries += 1
                        logger.info(f"Trovati {results_found} risultati per la query: {query}")
                    else:
                        logger.warning(f"Nessun risultato valido trovato per la query: {query}")
                else:
                    logger.warning(f"Risposta non valida per la query '{query}': status code {response.status_code}")
                    logger.debug(f"Headers risposta: {response.headers}")
                
            except Exception as e:
                logger.error(f"Errore nella ricerca per query '{query}': {str(e)}")
                continue
        
        # Log del riepilogo
        logger.info(f"Riepilogo ricerca per {company_name}:")
        logger.info(f"- Query totali eseguite: {total_queries}")
        logger.info(f"- Query con risultati: {successful_queries}")
        logger.info(f"- Risultati totali trovati: {len(search_results)}")
        
        # Scrivi il log CSV con tutti i dettagli
        if log_rows:
            try:
                file_exists = os.path.isfile(log_file)
                logger.info(f"Scrittura {len(log_rows)} righe nel file di log: {log_file}")
                
                with open(log_file, mode='a', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ["azienda", "query", "title", "snippet", "url"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if not file_exists:
                        writer.writeheader()
                        logger.info("Creato nuovo file di log con intestazione")
                    writer.writerows(log_rows)
                    logger.info(f"Scrittura completata nel file: {log_file}")
            except Exception as e:
                logger.error(f"Errore nella scrittura del file di log: {str(e)}")
        else:
            logger.warning(f"Nessun risultato da loggare per {company_name}")
        
        return search_results
        
    except Exception as e:
        logger.error(f"Errore generale nella ricerca contatti per {company_name}: {str(e)}")
        return []

def extract_admin_with_gpt(texts):
    """Estrae il nome dell'amministratore dai testi usando GPT"""
    if not OPENAI_API_KEY:
        return None
    try:
        prompt = f"""Analizza i testi e estrai il nome dell'amministratore o legale rappresentante.
        Rispondi SOLO con il nome. Se non trovi, rispondi 'Nessun amministratore trovato'.

        Testi:
        {texts}"""

        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Sei un assistente specializzato nell'estrazione di nomi di amministratori."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=60
        )
        logging.info(f"GPT response: {response}")
        admin_name = response.choices[0].message.content.strip()
        if any(phrase in admin_name.lower() for phrase in ["nessun amministratore", "non ho trovato", "non è possibile"]):
            return None
        return admin_name
    except Exception as e:
        logger.error(f"GPT extraction error: {e}")
        return None

def test_single_query(driver, company_name, query):
    """Test di una singola query di ricerca con Selenium con gestione CAPTCHA e cookie rapida."""
    if driver is None: 
        logger.error("Driver non inizializzato")
        return None
    try:
        # Usa un URL di ricerca più "umano"
        encoded_query = query.replace(' ', '+')
        url = f"https://www.google.com/search?q={encoded_query}&hl=it&gl=it"
        logger.info(f"Caricamento URL di ricerca: {url}")
        driver.get(url)
        time.sleep(random.uniform(2, 4))
        
        # Verifica immediata CAPTCHA
        page_source = driver.page_source.lower()
        if "captcha" in page_source or "recaptcha" in page_source or "g-recaptcha" in page_source:
            logger.error(f"ATTENZIONE: Google ha bloccato la ricerca con un CAPTCHA per {company_name}")
            try:
                os.makedirs("debug_screenshots", exist_ok=True)
                debug_path = os.path.join("debug_screenshots", f"debug_captcha_{int(time.time())}.png")
                driver.save_screenshot(debug_path)
                logger.info(f"Screenshot del CAPTCHA salvato in: {debug_path}")
            except:
                pass
            return None

        # Gestione consenso cookie rapida (un unico wait per tutti i selettori comuni)
        cookie_selectors = [
            "button#L2AGLb",
            "button[aria-label='Accetta tutto']",
            "button[aria-label='Accept all']",
            "button[aria-label='I agree']",
            "button[jsname='tWT92d']",
            "button[jsname='ZUkOIc']",
            "#L2AGLb"
        ]
        
        combined_selector = ", ".join(cookie_selectors)
        try:
            # Aspetta che ALMENO UNO dei pulsanti sia cliccabile
            btn = WebDriverWait(driver, 4).until(EC.element_to_be_clickable((By.CSS_SELECTOR, combined_selector)))
            driver.execute_script("arguments[0].click();", btn)
            logger.info("Consenso cookie superato con successo.")
            time.sleep(random.uniform(1, 2))
        except Exception:
            # Se non lo trova entro 4 secondi, probabilmente non c'è o è già stato accettato
            logger.debug("Nessun pulsante cookie trovato rapidamente, proseguo.")
        
        all_texts = []
        result_selectors = [
            "div.g",
            "div[data-hveid]",
            "div.yuRUbf",
            "div.VwiC3b",  # Snippet testuale
            "div[jscontroller]"
        ]
        
        # Prova a estrarre testi dai risultati
        found_elements = []
        for selector in result_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements and len(elements) > 2:
                    found_elements = elements
                    logger.info(f"Risultati trovati con selettore: {selector} (count: {len(elements)})")
                    break
            except:
                continue

        if not found_elements:
            # Fallback estremo: prendi tutti i paragrafi o div con testo significativo
            try:
                logger.warning("Nessun selettore standard ha prodotto risultati. Provo fallback generico.")
                found_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'VwiC3b') or contains(@class, 'yXB3nd')]")
            except:
                pass

        for res in found_elements[:5]:
            try:
                text = res.text.strip()
                if text and len(text) > 20: # Filtra testi troppo corti (menù, ecc.)
                    all_texts.append(text)
            except:
                continue
            
        if all_texts:
            logger.info(f"Estratti {len(all_texts)} frammenti di testo. Invio a GPT per {company_name}...")
            logger.info("=== TESTO INVIATO A GPT (START) ===")
            for idx, frag in enumerate(all_texts, 1):   # oppure all_texts, a seconda del nome variabile
                logger.info(f"[FRAG {idx}] {frag}")
            logger.info("=== TESTO INVIATO A GPT (END) ===")
            texts_combined = "\n\n".join(all_texts)
            return extract_admin_with_gpt(texts_combined)
        else:
            logger.error(f"Nessun testo estratto dalla pagina di Google per {company_name}. Pagina vuota o bloccata?")
            try:
                os.makedirs("debug_screenshots", exist_ok=True)
                debug_path = os.path.join("debug_screenshots", f"debug_empty_{int(time.time())}.png")
                driver.save_screenshot(debug_path)
                logger.info(f"Screenshot della pagina vuota salvato in: {debug_path}")
            except:
                pass
            # Se siamo qui, il CAPTCHA non è stato rilevato ma i risultati mancano.
            # Potrebbe essere utile loggare il titolo della pagina.
            logger.info(f"Titolo pagina: {driver.title}")
            
    except Exception as e:
        logger.error(f"Errore in test_single_query per {company_name}: {str(e)}")
    return None

def extract_contact_person(company_info, api_key=None):
    """Estrae informazioni di contatto da Google Search usando Selenium (Standalone Legacy)"""
    company_name = company_info['nome']
    logger.info(f"\n{'='*50}\nRicerca contatti per: {company_name}\n{'='*50}")
    
    # Configurazione del driver con undetected_chromedriver
    try:
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-logging')
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-webgl2')
        options.add_argument('--ignore-certificate-errors')
        
        # User agent realistico
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Opzioni anti-rilevamento
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')
        options.add_argument('--disable-site-isolation-trials')
        
        driver = uc.Chrome(options=options)
        driver.set_page_load_timeout(30)
        driver.implicitly_wait(3) 
        
    except Exception as e:
        logger.error(f"Errore nella configurazione del driver per {company_name}: {str(e)}")
        return company_info
    
    try:
        # Costruisci la query di ricerca
        query = f"{company_name} amministratore"
        logger.info(f"Esecuzione query per {company_name}: {query}")
        
        # Costruisci l'URL di ricerca
        encoded_query = query.replace(' ', '+')
        url = f"https://www.google.com/search?q={encoded_query}&hl=it"
        
        # Carica la pagina
        driver.get(url)
        logger.info(f"Pagina caricata per {company_name}")
        
        # Attesa per il caricamento iniziale (ridotta per velocità)
        time.sleep(random.uniform(2, 3))
        
        # Gestione consenso cookie con più selettori
        cookie_selectors = [
            (By.ID, "L2AGLb"),
            (By.CSS_SELECTOR, "button[aria-label='Accetta tutto']"),
            (By.CSS_SELECTOR, "button[aria-label='Accept all']"),
            (By.CSS_SELECTOR, "button[aria-label='I agree']"),
            (By.CSS_SELECTOR, "button#L2AGLb"),
            (By.CSS_SELECTOR, "button[jsname='tWT92d']"),
            (By.CSS_SELECTOR, "button[jsname='ZUkOIc']"),
            (By.CSS_SELECTOR, "button[jsname='tWT92d']")
        ]
        
        for selector_type, selector in cookie_selectors:
            try:
                consent_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((selector_type, selector))
                )
                driver.execute_script("arguments[0].click();", consent_button)
                time.sleep(random.uniform(1, 1.5))
                logger.info(f"Cookie accettati per {company_name}")
                break
            except:
                continue
        
        # Attendi il caricamento dei risultati
        time.sleep(random.uniform(2, 3))
        
        # Prova diversi selettori per i risultati
        selectors_to_try = [
            (By.CSS_SELECTOR, "div#search"),
            (By.CSS_SELECTOR, "div[role='main']"),
            (By.CSS_SELECTOR, "div#rso"),
            (By.CSS_SELECTOR, "div#search"),
            (By.CSS_SELECTOR, "div.g"),
            (By.CSS_SELECTOR, "div[data-hveid]"),
            (By.CSS_SELECTOR, "div[data-sokoban-container]"),
            (By.CSS_SELECTOR, "div[jscontroller]")
        ]
        
        search_div = None
        for selector_type, selector in selectors_to_try:
            try:
                search_div = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((selector_type, selector))
                )
                if search_div:
                    logger.info(f"Elemento trovato con selettore: {selector} per {company_name}")
                    break
            except:
                continue
        
        if search_div:
            # Prova diversi selettori per i risultati
            result_selectors = [
                "div.g",
                "div[data-hveid]",
                "div.rc",
                "div.yuRUbf",
                "div[data-sokoban-container]",
                "div[jscontroller]",
                "div[data-content-feature='1']",
                "div[data-content-feature='2']",
                "div[data-content-feature='3']"
            ]
            
            results = []
            for selector in result_selectors:
                try:
                    results = search_div.find_elements(By.CSS_SELECTOR, selector)
                    if results:
                        logger.info(f"Risultati trovati con selettore: {selector} per {company_name}")
                        break
                except:
                    continue
            
            logger.info(f"Numero di risultati trovati per {company_name}: {len(results)}")
            
            # Analizza i primi 3 risultati
            for i, result in enumerate(results[:3], 1):
                try:
                    # Estrai titolo e snippet
                    title = None
                    for title_selector in ["h3", ".LC20lb", ".DKV0Md", "div[role='heading']", "div[data-content-feature='1']"]:
                        try:
                            title = result.find_element(By.CSS_SELECTOR, title_selector).text
                            if title:
                                break
                        except:
                            continue
                    
                    snippet = None
                    for snippet_selector in ["div.VwiC3b", "div.IsZvec", "span.st", "div[data-content-feature='1']", "div[data-content-feature='2']"]:
                        try:
                            snippet = result.find_element(By.CSS_SELECTOR, snippet_selector).text
                            if snippet:
                                break
                        except:
                            continue
                    
                    if title or snippet:
                        logger.info(f"Analisi risultato {i} per {company_name}:")
                        logger.info(f"Titolo: {title}")
                        logger.info(f"Snippet: {snippet}")
                        
                        # Cerca email nel titolo e snippet
                        text_to_search = f"{title or ''} {snippet or ''}"
                        
                        # Cerca email
                        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                        emails = re.findall(email_pattern, text_to_search)
                        
                        # Cerca LinkedIn
                        linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[a-zA-Z0-9%_-]+/?'
                        linkedin_links = re.findall(linkedin_pattern, text_to_search)
                        
                        # Aggiorna company_info se troviamo nuovi contatti
                        if emails:
                            existing_emails = company_info.get('email', '').split(', ')
                            new_emails = [email for email in emails if email not in existing_emails]
                            if new_emails:
                                company_info['email'] = ', '.join(existing_emails + new_emails)
                                logger.info(f"Nuove email trovate per {company_name}: {new_emails}")
                        
                        if linkedin_links:
                            existing_linkedin = company_info.get('linkedin', '').split(', ')
                            new_linkedin = [link for link in linkedin_links if link not in existing_linkedin]
                            if new_linkedin:
                                company_info['linkedin'] = ', '.join(existing_linkedin + new_linkedin)
                                logger.info(f"Nuovi link LinkedIn trovati per {company_name}: {new_linkedin}")
                        
                except Exception as e:
                    logger.error(f"Errore nell'estrazione del risultato {i} per {company_name}: {str(e)}")
        else:
            logger.error(f"Nessun elemento di ricerca trovato per {company_name}!")
            logger.info(f"Contenuto della pagina per {company_name}:")
            logger.info(f"Titolo: {driver.title}")
            logger.info(f"URL attuale: {driver.current_url}")
            try: 
                 logger.info(f"Primi 500 caratteri del body: {driver.find_element(By.TAG_NAME, 'body').text[:500]}")
            except: pass
        
    except Exception as e:
        logger.error(f"Errore generale nella ricerca contatti per {company_name}: {str(e)}")
    
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
    
    return company_info

#---------------------------------------------------------------------------
# DuckDuckGo Search Amministratore

def ddg_search_snippets(query: str, max_results: int = 5, timeout: int = 20, retries: int = 3):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "DNT": "1",
        "Referer": "https://duckduckgo.com/",
        "Upgrade-Insecure-Requests": "1",
    }

    # Endpoint HTML parsabile: meglio POST che GET
    url_html = "https://html.duckduckgo.com/html/"
    data = {"q": query}

    last_status = None
    html_text = ""

    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url_html, headers=headers, data=data, timeout=timeout)
            last_status = r.status_code
            html_text = r.text or ""
        except Exception as e:
            logger.error(f"[ddg] errore richiesta (attempt {attempt}/{retries}): {e}")
            html_text = ""

        # 200 con body -> ok
        if last_status == 200 and html_text.strip():
            break

        # 202/empty -> backoff e riprova
        if last_status in (202, 429) or not html_text.strip():
            sleep_s = random.uniform(6, 12) * attempt
            logger.warning(f"[ddg] status={last_status} risposta vuota (attempt {attempt}/{retries}) -> sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        # altri status: esci e prova fallback sotto
        break

    # Fallback: DuckDuckGo LITE (meno blocchi, HTML diverso ma parsabile)
    if not (last_status == 200 and html_text.strip()):
        url_lite = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
        try:
            r2 = requests.get(url_lite, headers=headers, timeout=timeout)
            if r2.status_code == 200 and (r2.text or "").strip():
                html_text = r2.text
                last_status = 200
                logger.info("[ddg] fallback LITE ok")
            else:
                logger.warning(f"[ddg] fallback LITE status={r2.status_code} vuoto")
                return []
        except Exception as e:
            logger.error(f"[ddg] fallback LITE errore: {e}")
            return []

    soup = BeautifulSoup(html_text, "html.parser")
    results = []

    # Parser per endpoint HTML classico
    for res in soup.select("div.result"):
        a = res.select_one("a.result__a")
        if not a:
            continue
        title = (a.get_text(" ", strip=True) or "").strip()
        href = (a.get("href") or "").strip()
        sn = res.select_one(".result__snippet")
        snippet = (sn.get_text(" ", strip=True) if sn else "").strip()

        if title or snippet:
            results.append({"title": title, "snippet": snippet, "url": href})
        if len(results) >= max_results:
            break

    # Se siamo finiti sul LITE, la struttura è diversa: prendi link + testo riga
    if not results:
        # nel LITE spesso i risultati sono in <a> dentro tabelle
        for a in soup.select("a"):
            href = (a.get("href") or "").strip()
            title = (a.get_text(" ", strip=True) or "").strip()
            if href.startswith("http") and title:
                results.append({"title": title, "snippet": "", "url": href})
            if len(results) >= max_results:
                break

    logger.info(f"[ddg] risultati trovati: {len(results)} per query='{query}'")
    return results


def build_ddg_payload(company_name: str, query: str, ddg_results: list, max_items: int = 5) -> str:
    """
    Crea testo pulito da dare a GPT: titoli + snippet + url dei top risultati DDG.
    """
    lines = []
    lines.append(f"AZIENDA: {company_name}")
    lines.append(f"QUERY: {query}")
    lines.append("")
    lines.append("RISULTATI (DuckDuckGo):")

    for i, it in enumerate(ddg_results[:max_items], 1):
        title = (it.get("title") or "").strip()
        snippet = (it.get("snippet") or "").strip()
        url = (it.get("url") or "").strip()
        lines.append(f"\n[{i}] TITOLO: {title}")
        lines.append(f"[{i}] SNIPPET: {snippet}")
        lines.append(f"[{i}] URL: {url}")

    return "\n".join(lines).strip()


def admin_from_ddg_with_gpt(company_name: str, query: str, max_results: int = 5):
    """
    1) Cerca con DDG
    2) Costruisce payload (titoli/snippet/url)
    3) Lo manda a GPT usando extract_admin_with_gpt
    Ritorna: (admin_name, ddg_payload, ddg_results)
    """
    ddg_results = ddg_search_snippets(query, max_results=max_results)
    if not ddg_results:
        return None, "", []

    ddg_payload = build_ddg_payload(company_name, query, ddg_results, max_items=max_results)

    # LOG: questo è il testo ESATTO che va a GPT
    logger.info("=== DDG PAYLOAD → GPT (START) ===")
    logger.info(ddg_payload[:4000])   # evita log infiniti
    logger.info("=== DDG PAYLOAD → GPT (END) ===")

    admin_name = extract_admin_with_gpt(ddg_payload)
    return admin_name, ddg_payload, ddg_results
