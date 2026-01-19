import requests
import time
import random
import logging
from urllib.parse import quote_plus
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class DDGSearcher:
    """DuckDuckGo searcher con rate limiting intelligente e fallback multipli"""
    
    def __init__(self):
        self.session = requests.Session()
        self.last_request_time = 0
        self.min_delay = 8.0  # Delay minimo tra richieste
        self.backoff_factor = 1.5
        self.max_delay = 60.0
        self.current_delay = self.min_delay
        self.consecutive_failures = 0
        
        # User agents realistici
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
        ]
    
    def _wait_with_jitter(self):
        """Aspetta con delay progressivo e jitter"""
        elapsed = time.time() - self.last_request_time
        wait_time = max(0, self.current_delay - elapsed)
        
        if wait_time > 0:
            jitter = wait_time * random.uniform(-0.2, 0.2)
            actual_wait = wait_time + jitter
            logger.info(f"[DDG] Waiting {actual_wait:.1f}s before next request")
            time.sleep(actual_wait)
        
        self.last_request_time = time.time()
    
    def _get_headers(self):
        """Headers realistici con user agent rotante"""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            # RIMOSSO Accept-Encoding per evitare gzip (requests lo gestisce automaticamente)
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0"
        }
    
    def _handle_response(self, status_code: int, content_length: int = 0) -> bool:
        """Gestisce la risposta e aggiorna il backoff. Ritorna True se OK."""
        if status_code == 200 and content_length > 100:
            # Successo - riduci gradualmente il delay
            self.consecutive_failures = 0
            self.current_delay = max(self.min_delay, self.current_delay * 0.9)
            logger.info(f"[DDG] ✓ Success! Content: {content_length} chars, Delay: {self.current_delay:.1f}s")
            return True
        
        elif status_code in (202, 429):
            # Rate limiting - aumenta il delay
            self.consecutive_failures += 1
            self.current_delay = min(
                self.max_delay,
                self.current_delay * (self.backoff_factor ** self.consecutive_failures)
            )
            logger.warning(f"[DDG] ✗ Rate limited (status {status_code}). Delay now: {self.current_delay:.1f}s")
            return False
        
        elif status_code == 200 and content_length <= 100:
            logger.warning(f"[DDG] ✗ Empty response (only {content_length} chars)")
            return False
        
        else:
            logger.error(f"[DDG] ✗ Unexpected status: {status_code}")
            return False
    
    def search_lite(self, query: str, max_results: int = 5) -> list:
        """
        Usa DDG Lite - interfaccia minimale, meno probabilità di blocco
        """
        self._wait_with_jitter()
        
        url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
        
        try:
            logger.info(f"[DDG Lite] GET: {url[:80]}...")
            
            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=20
            )
            
            # Log dettagliato della risposta
            logger.info(f"[DDG Lite] Status: {response.status_code}")
            logger.info(f"[DDG Lite] Content-Type: {response.headers.get('Content-Type', 'N/A')}")
            logger.info(f"[DDG Lite] Content-Length: {len(response.text)} chars")
            
            # DEBUG: Log primi caratteri della risposta
            preview = response.text[:500] if response.text else "(empty)"
            logger.debug(f"[DDG Lite] Response preview: {preview}")
            
            if not self._handle_response(response.status_code, len(response.text)):
                return []
            
            soup = BeautifulSoup(response.text, "html.parser")
            results = []
            
            # DEBUG: Log della struttura HTML
            tables = soup.find_all('table')
            logger.info(f"[DDG Lite] Found {len(tables)} tables")
            
            # Parser per DDG Lite
            for table in tables:
                rows = table.find_all('tr')
                logger.info(f"[DDG Lite] Processing table with {len(rows)} rows")
                
                for tr in rows:
                    # Cerca link con classe result-link
                    link = tr.find('a', class_='result-link')
                    if not link:
                        # Fallback: qualsiasi link
                        link = tr.find('a', href=True)
                    
                    if not link:
                        continue
                    
                    title = link.get_text(strip=True)
                    href = link.get('href', '')
                    
                    # Cerca snippet
                    snippet_td = tr.find('td', class_='result-snippet')
                    if not snippet_td:
                        # Fallback: seconda td
                        tds = tr.find_all('td')
                        snippet_td = tds[1] if len(tds) > 1 else None
                    
                    snippet = snippet_td.get_text(strip=True) if snippet_td else ""
                    
                    # Filtra risultati validi
                    if title and href and href.startswith('http'):
                        results.append({
                            'title': title,
                            'snippet': snippet,
                            'url': href
                        })
                        logger.debug(f"[DDG Lite] ✓ Found: {title[:50]}...")
                        
                        if len(results) >= max_results:
                            break
                
                if len(results) >= max_results:
                    break
            
            logger.info(f"[DDG Lite] Total results: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"[DDG Lite] Exception: {e}", exc_info=True)
            return []
    
    def search_html(self, query: str, max_results: int = 5) -> list:
        """
        Usa DDG HTML (POST) - più affidabile del Lite
        """
        self._wait_with_jitter()
        
        url = "https://html.duckduckgo.com/html/"
        data = {"q": query}
        
        try:
            logger.info(f"[DDG HTML] POST with query: {query[:50]}...")
            
            response = self.session.post(
                url,
                headers=self._get_headers(),
                data=data,
                timeout=20
            )
            
            # Log dettagliato
            logger.info(f"[DDG HTML] Status: {response.status_code}")
            logger.info(f"[DDG HTML] Content-Type: {response.headers.get('Content-Type', 'N/A')}")
            logger.info(f"[DDG HTML] Content-Length: {len(response.text)} chars")
            
            # DEBUG: Log primi caratteri
            preview = response.text[:500] if response.text else "(empty)"
            logger.debug(f"[DDG HTML] Response preview: {preview}")
            
            if not self._handle_response(response.status_code, len(response.text)):
                return []
            
            soup = BeautifulSoup(response.text, "html.parser")
            results = []
            
            # DEBUG: Conta elementi trovati
            result_divs = soup.select('div.result')
            logger.info(f"[DDG HTML] Found {len(result_divs)} result divs")
            
            for idx, result_div in enumerate(result_divs[:max_results], 1):
                title_elem = result_div.select_one('a.result__a')
                if not title_elem:
                    logger.debug(f"[DDG HTML] Result {idx}: No title element")
                    continue
                
                title = title_elem.get_text(strip=True)
                href = title_elem.get('href', '')
                
                snippet_elem = result_div.select_one('.result__snippet')
                snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                
                if title:
                    results.append({
                        'title': title,
                        'snippet': snippet,
                        'url': href
                    })
                    logger.debug(f"[DDG HTML] ✓ Result {idx}: {title[:50]}...")
            
            logger.info(f"[DDG HTML] Total results: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"[DDG HTML] Exception: {e}", exc_info=True)
            return []
    
    def search(self, query: str, max_results: int = 5) -> list:
        """
        Cerca con fallback: prima HTML, poi Lite
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"[DDG] Starting search for: {query}")
        logger.info(f"{'='*60}")
        
        # Prova prima HTML (più completo)
        results = self.search_html(query, max_results)
        
        # Se fallisce, prova Lite
        if not results:
            logger.warning("[DDG] HTML failed, trying Lite...")
            time.sleep(random.uniform(3, 6))
            results = self.search_lite(query, max_results)
        
        if results:
            logger.info(f"[DDG] ✓ Successfully found {len(results)} results")
        else:
            logger.warning(f"[DDG] ✗ No results found for query: {query}")
        
        return results


# Istanza globale con stato persistente
_ddg_searcher = DDGSearcher()

def ddg_search_improved(query: str, max_results: int = 5) -> list:
    """
    Funzione wrapper compatibile con il codice esistente
    """
    return _ddg_searcher.search(query, max_results)


# Test standalone
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    
    searcher = DDGSearcher()
    
    test_queries = [
        "python programming",
        "openai gpt",
        "machine learning tutorial"
    ]
    
    for query in test_queries:
        print(f"\n{'='*80}")
        print(f"Testing: {query}")
        print('='*80)
        
        results = searcher.search(query, max_results=3)
        
        print(f"\nResults: {len(results)}")
        for i, r in enumerate(results, 1):
            print(f"\n[{i}] {r['title']}")
            print(f"    {r['snippet'][:100]}...")
            print(f"    {r['url']}")
        
        time.sleep(5)  # Pausa tra test