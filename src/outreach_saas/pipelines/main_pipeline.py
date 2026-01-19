import logging
import time
import os
import sys
import argparse
import requests
from typing import List, Optional

from ..config.settings import (
    GOOGLE_PLACES_API_KEY,
    SCRAPING_METHOD 
)
from ..config.definitions import (
    INDUSTRY_CONFIG,
    KEYWORDS_BY_INDUSTRY
)
from ..utils.geo_utils import (
    load_comuni, 
    build_comune_coordinates_dict,
    generate_search_urls_with_coordinates,
    filter_and_limit_comuni,
    _load_processed_comuni,
    _save_processed_comuni
)
from ..utils.file_utils import (
    load_existing_keys, 
    save_to_csv, 
    create_backup, 
    append_result_to_csv,
    #log_api_usage
)
from ..scraping.driver_utils import init_driver_helper, cleanup_chrome_tmp
from ..scraping.selenium_scraper import scrape_with_selenium
from ..scraping.places_scraper import scrape_with_places_api
from ..scraping.serpapi_scraper import scrape_with_serpapi_geo
from ..scraping.search_utils import search_contact_info, extract_admin_with_gpt, test_single_query, extract_emails_from_website, extract_emails_with_driver
from ..analysis.relevance_analyzer import WebsiteRelevanceAnalyzer
from ..utils.text_utils import clean_url
from ..config.definitions import KEYWORDS_BY_INDUSTRY, INDUSTRY_CONFIG, BIG_COMPANY_KEYWORDS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def is_site_reachable(url, timeout=5):
    """Verifica rapidamente se un sito è raggiungibile prima di tentare l'estrazione email."""
    if not url or not url.startswith(('http://', 'https://')):
        return False
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        # Accetta 2xx e 3xx, accetta anche 4xx (potrebbero esserci contenuti validi)
        # Rifiuta solo 5xx (server error) e timeout/connection errors
        return response.status_code < 500
    except (requests.exceptions.ConnectionError, 
            requests.exceptions.Timeout,
            requests.exceptions.RequestException):
        logger.debug(f"Sito non raggiungibile: {url}")
        return False
    except Exception as e:
        logger.debug(f"Errore verifica raggiungibilità {url}: {e}")
        return False

def deduplicate_results(results):
    """Rimuove duplicati dalla lista dei risultati basandosi su Nome e Comune."""
    unique_results = []
    seen = set()
    for res in results:
        key = (
            str(res.get('nome','')).strip().lower(), 
            str(res.get('comune','')).strip().lower()
        )
        if key not in seen:
            seen.add(key)
            unique_results.append(res)
    return unique_results

def enrich_and_filter_results(results, analyzer, driver=None, output_file=None, existing_keys=None, enable_admin_enrichment=False):
    """
    Arricchisce i risultati con email, analisi pertinenza, e filtro.
    Salva progressivamente se output_file è fornito.
    """
    from ..scraping.search_utils import extract_emails_from_website, extract_emails_with_driver
    
    enriched_results = []
    
    if existing_keys is None:
        existing_keys = set()
        
    # Headers per salvataggio incrementale
    fieldnames = [
        "comune", "keyword", "nome", "indirizzo", "telefono", "sito_web",
        "email", "linkedin", "pertinenza", "categoria", 
        "confidenza_analisi", "contatto", "num_recensioni", "tipo", "distanza_km"
    ]
    
    logger.info(f"Inizio arricchimento per {len(results)} risultati...")
    
    for i, result in enumerate(results):
        try:
            nome = str(result.get('nome', '')).strip()
            comune = str(result.get('comune', '')).strip()
            sito = result.get('sito_web', '')
            
            # 0. Check Big Company (Legacy Logic)
            nome_lower = nome.lower()
            is_big_company = any(kw in nome_lower for kw in BIG_COMPANY_KEYWORDS)
            if is_big_company:
                logger.info(f"Saltata grande impresa: {nome}")
                continue

            # 0.5 Clean URL (Legacy Logic)
            sito = clean_url(sito) if sito else ''
            
            # Filtro siti non validi (Google Maps link residui)
            if sito and ("google.com" in sito.lower() or "google.it" in sito.lower()):
                logger.info(f"Sito non valido (Google redirect) per {nome}: {sito}")
                sito = ""
            
            result['sito_web'] = sito

            # Check duplicati (normalizzato)
            key_check = (
                nome.lower().strip(), 
                comune.lower().strip(), 
                sito.lower().strip().rstrip('/')
            )
            
             # Verifica esistenza (nome+comune)
            if any(k[0] == key_check[0] and k[1] == key_check[1] for k in existing_keys):
                logger.info(f"Skipping duplicate: {nome} ({comune})")
                continue

            logger.info(f"[{i+1}/{len(results)}] Analisi: {nome}")
            
            # 1. Trova Admin (DISATTIVATO DI DEFAULT per non triggerare CAPTCHA nella main)
            admin_name = ""
            if enable_admin_enrichment:
                if not driver:
                    # Fallback HTTP
                    try:
                        snippet_results = search_contact_info(nome) or []
                        texts = []
                        for item in snippet_results:
                            t = item.get("title")
                            s = item.get("snippet")
                            if t: texts.append(t)
                            if s: texts.append(s)
                        admin_name = extract_admin_with_gpt("\n".join(texts)) if texts else None
                    except Exception as e:
                        logger.error(f"Errore nel fallback HTTP per admin di {nome}: {e}")
                else:
                    # Selenium
                    try:
                        logger.info(f"Ricerca amministratore per {nome}")
                        admin_name = test_single_query(driver, nome, f"{nome} amministratore")
                    except Exception as e:
                        logger.error(f"Errore ricerca amministratore per {nome}: {e}")
                        admin_name = None

            result['contatto'] = admin_name or ""

            # 2. Analisi pertinenza sito
            pertinenza = False
            categoria = "Sconosciuto"
            confidenza = 0.0
            
            if sito and len(sito) > 5:
                res_analysis = analyzer.analyze_website_relevance(sito)
                pertinenza = res_analysis.get('is_relevant', False)
                categoria = res_analysis.get('category', 'Sconosciuto')
                confidenza = res_analysis.get('confidence', 0.0)
            
            result['pertinenza'] = pertinenza
            result['categoria'] = categoria
            result['confidenza_analisi'] = confidenza
            
            # Pulizia finale indirizzo e telefono (rimozione icone Google)
            from ..utils.text_utils import clean_extracted_text
            result['indirizzo'] = clean_extracted_text(result.get('indirizzo', ''))
            result['telefono'] = clean_extracted_text(result.get('telefono', ''))
            
            # Filtro rapido legacy (manteniamo?) o salviamo tutto?
            # Legacy default: salva tutto ma logga pertinenza.
            
            # 3. Estrazione Email (Legacy flow)
            # Estrai email e LinkedIn anche se non pertinente (come legacy)
            emails = []
            linkedin_links = []
            
            if sito and pertinenza:
                # Pre-check: verifica se il sito è raggiungibile prima di tentare l'estrazione
                if is_site_reachable(sito):
                    logger.info(f"Estrazione email/LinkedIn da: {sito}")
                    
                    # A. HTTP Request
                    emails = extract_emails_from_website(sito, disable_slug_fallback=False) or []
                    
                    # B. Selenium Fallback
                    if not emails and driver:
                        logger.info("Nessuna email trovata via HTTP. Provo fallback Selenium...")
                        try:
                            selenium_emails = extract_emails_with_driver(sito, driver)
                            emails.extend(selenium_emails)
                        except Exception as e:
                            logger.debug(f"Fallback Selenium per email fallito: {e}")
                else:
                    logger.warning(f"Sito non raggiungibile, skip estrazione email: {sito}")

            # Deduplica e formatta
            unique_emails = []
            linkedin_links = []
            for item in emails:
                if isinstance(item, str) and item.startswith("LINKEDIN:"):
                    linkedin_links.append(item.replace("LINKEDIN:", ""))
                elif isinstance(item, str):
                    e = item.lower().strip()
                    if e not in unique_emails:
                        unique_emails.append(e)

            result['email'] = ", ".join(unique_emails)
            result['linkedin'] = ", ".join(linkedin_links)
            
            # Assicurati che tutti i campi siano presenti per il DictWriter
            for field in fieldnames:
                if field not in result:
                    result[field] = ""
            
            enriched_results.append(result)
            
            # Salvataggio incrementale
            if output_file:
                append_result_to_csv(result, output_file, fieldnames)
                existing_keys.add(key_check)
                
        except Exception as e:
            logger.error(f"Errore arricchimento {result.get('nome')}: {e}")
            
    return enriched_results

def run_pipeline(industry="fotovoltaico", input_comuni_file=None, test_email=False):
    logger.info("=== AVVIO PIPELINE SCRAPER ===")
    
    # 1. Configurazione Industria
    # Se l'industria passata non è in config, fallback o errore?
    # Usiamo "fotovoltaico" come default sicuro se non trovata la chiave esatta
    industry_key = industry.lower() if industry.lower() in INDUSTRY_CONFIG else "fotovoltaico"
    keywords = KEYWORDS_BY_INDUSTRY.get(industry_key, KEYWORDS_BY_INDUSTRY["fotovoltaico"])
    logger.info(f"Target Industry: {industry_key}") 
    logger.info(f"Keywords: {keywords}")

    # 2. Caricamento Comuni
    comuni = []
    if input_comuni_file and os.path.exists(input_comuni_file):
        logger.info(f"Leggo comuni da: {input_comuni_file}")
        # Se è csv, usa load_comuni (che si aspetta colonne specifiche), se è txt listato?
        # load_comuni gestisce csv. Se fosse txt semplice:
        if input_comuni_file.endswith('.txt'):
            with open(input_comuni_file, 'r') as f:
                comuni = [l.strip() for l in f if l.strip()]
        else:
            comuni = load_comuni(input_comuni_file)
    else:
        # Fallback o interattivo (ma qui siamo in pipeline)
        logger.warning("Nessun file comuni fornito. Uso default 'data/comuni_italiani.csv' se esiste.")
        comuni = load_comuni()

    if not comuni:
        logger.error("Nessun comune caricato. Uscita.")
        return

    # Comuni processati e limit
    processed_comuni = _load_processed_comuni()
    comuni_da_processare = filter_and_limit_comuni(comuni, processed_comuni) # limit opzionale
    
    if not comuni_da_processare:
        logger.info("Tutti i comuni sono già processati.")
        return

    # 3. Coordinate e URL
    coords_dict = build_comune_coordinates_dict(comuni_da_processare)
    search_urls = generate_search_urls_with_coordinates(comuni_da_processare, keywords, coords_dict)
    logger.info(f"Generati {len(search_urls)} URL di ricerca.")

    # 4. Selezione Metodo Scraping
    # Logica originale: preferisce SerpAPI se c'è key, poi Selenium/Places
    results = []
    driver = None
    
    # Definiamo output file
    output_filename = f"aziende_{industry_key}_filtrate.csv"
    output_path = os.path.join("output", output_filename) # o cwd
    if not os.path.exists("output"): os.makedirs("output", exist_ok=True)
    
    # Carica chiavi esistenti per deduplica globale
    existing_keys = load_existing_keys(output_path)

    # Nota: Places API è il metodo "consigliato" nel nuovo app.py se c'è la key
    use_places = bool(GOOGLE_PLACES_API_KEY)
    scraping_method = SCRAPING_METHOD
    if scraping_method == "places":
        logger.info("Usando Google Places API...")
        # Usa places_scraper
        #results, counters = scrape_with_places_api(search_urls, GOOGLE_PLACES_API_KEY, fetch_details=True, per_query_limit=20)
        #log_api_usage("GooglePlaces", counters.get("nearby_requests", 0) + counters.get("details_requests", 0))
    elif scraping_method == "selenium":
        logger.info("Usando Selenium Scraper...")
        # Usa selenium_scraper
        # Init driver
        try:
            driver = init_driver_helper(headless=True)
            results, driver = scrape_with_selenium(search_urls, coords_dict, driver=driver)
        except Exception as e:
            logger.error(f"Errore Selenium: {e}")
            if driver: driver.quit()
            return

    # 5. Deduplica
    logger.info(f"Risultati grezzi: {len(results)}")
    unique_results = deduplicate_results(results)
    logger.info(f"Risultati unici: {len(unique_results)}")
    
    # 6. Arricchimento
    analyzer = WebsiteRelevanceAnalyzer(industry=industry_key)
    
    # Se abbiamo usato Places, non abbiamo un driver aperto. Se serve per email fallback, lo apriamo.
    if not driver and unique_results: # e vogliamo usare driver per email fallback
        # Solo se strettamente necessario. Per ora proviamo senza driver per risparmiare risorse, 
        # o lo apriamo on demand. 
        # enriched_results usa requests prima. Se fallisce e driver è None, skippa fallback selenium.
        pass

    enriched = enrich_and_filter_results(
        unique_results, analyzer, driver,
        output_file=output_path,
        existing_keys=existing_keys,
        enable_admin_enrichment=False   # main pulita
    )
    # Nota: Places API è il metodo "consigliato" nel nuovo app.py se c'è la key

    logger.info(f"Completato. Totale risultati arricchiti salvati: {len(enriched)}")
    
    # Aggiorna processati
    processed_set = set(processed_comuni)
    for c in comuni_da_processare:
        processed_set.add(c)
    _save_processed_comuni(processed_set)
    
    if driver:
        driver.quit()
    
    # Pulizia finale file temporanei
    cleanup_chrome_tmp()

def main():
    parser = argparse.ArgumentParser(description="Outreach SaaS Scraper Pipeline")
    parser.add_argument("--industry", type=str, default="fotovoltaico", help="Settore target")
    parser.add_argument("--input", type=str, default="prova.csv", help="File input comuni (CSV o TXT)")
    args = parser.parse_args()
    
    # Gestione input interattivo per compatibilità legacy se arg non fornito
    input_file = args.input
    if not input_file:
        # Tenta di leggere da stdin se non siamo in un terminale interattivo o se è pipe?
        # App.py manda il nome file via stdin.
        # Controlliamo se c'è input su stdin (non bloccante è difficile in cross-platform python puro semplice)
        # Ma app.py fa: stdin_payload = f"{comuni_path.name}\n"
        # Quindi possiamo usare input() con timeout o semplicemente input() se sappiamo che viene chiamato così.
        # Per sicurezza, stampiamo il prompt che app.py si aspetta se vogliamo mantenere compatibilità al 100%
        # Ma app.py manda il nome file alla cieca?
        # App.py:
        # stdin_payload = f"{comuni_path.name}\n"
        # cmd = [PY_EXE, script]
        # stream_subprocess(..., input_data=stdin_payload)
        
        print("Inserisci nome file della lista comuni desiderata (es. comuni.csv):")
        try:
            line = sys.stdin.readline()
            if line:
                input_file = line.strip()
        except:
            pass
            
    run_pipeline(industry=args.industry, input_comuni_file=input_file)

if __name__ == "__main__":
    main()
