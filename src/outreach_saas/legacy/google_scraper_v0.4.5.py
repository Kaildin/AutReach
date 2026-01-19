import pandas as pd
import requests
import time
import math
import random
import csv
import re
import os
import logging
import openai
from urllib.parse import quote_plus, urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv  # Aggiungiamo questa importazione
import undetected_chromedriver as uc
from collections import deque
import json

# Carica le variabili dal file .env
load_dotenv()

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazioni
try:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]  # KeyError se non c'è
except KeyError:
    raise RuntimeError("Manca la variabile d'ambiente API_KEY nel .env")
API_KEY = "TUA_API_KEY_SERPAPI"  # Inserisci la tua API key di SerpAPI
GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", os.environ.get("GOOGLE_MAPS_API_KEY", ""))
OUTPUT_FILE = "aziende_fotovoltaico_filtrate.csv"
SCREENSHOT_DIR = "debug_screenshots"
PLACES_DETAILS_MODE = os.environ.get("PLACES_DETAILS_MODE", "web")  # "web" (default) oppure "details"

# Configura OpenAI
openai.api_key = OPENAI_API_KEY

# Modifica delle regex di email per maggiore precisione
EMAIL_PATTERNS = [
    r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
    r'mailto:\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
]

# Lista di keywords da cercare insieme al comune
KEYWORDS = [
    "fotovoltaico",
]

# Definizioni a livello di modulo per le liste di ignoranza
IGNORED_EMAIL_DOMAINS = {
    "sentry.io", "wixpress.com", "sentry.wixpress.com", 
    "sentrynext.wixpress.com", "users.wix.com",
    # Domini placeholder o di servizio generici
    "example.com", "test.com", "yourdomain.com", "mydomain.com",
    "website.com", "domain.com", "localhost",
    # Domini che spesso non sono contatti diretti dell'azienda target per lo scraping
    "google.com", "googleapis.com", "googleusercontent.com", "googlegroups.com",
    "facebook.com", "twitter.com", "instagram.com", # linkedin.com è gestito a parte
    "doubleclick.net", "googletagmanager.com", "googleadservices.com",
    "gstatic.com", "googlesyndication.com", "wixstatic.com",
    "amazonaws.com", "appspot.com", "cdn.com", "cloudfront.net",
    "windows.net", "azure.com", "microsoft.com", "msn.com", "outlook.com", "live.com", # Esempi, potrebbero essere troppo ampi
    "apple.com", "icloud.com",
    "yahoo.com", "aol.com", "mail.com" # Provider generici, spesso non per aziende specifiche
}

LOCAL_PART_IGNORE_PATTERNS = [
    re.compile(r"^[a-f0-9]{24,}$"),  # Stringa esadecimale lunga (>=24 caratteri, es. hash)
    re.compile(r"^[a-z0-9]{30,}$"), # Stringa alfanumerica generica lunga (>=30 caratteri)
    re.compile(r"^(noreply|no-reply|donotreply|unsubscribe|mailer-daemon|postmaster|abuse|bounces?|devnull|null)$"),
    re.compile(r"privacy|gdpr|legal|copyright", re.IGNORECASE), # Spesso indirizzi informativi, non di contatto primario
    re.compile(r"^.{1,2}@"), # Local part troppo corto (1 o 2 caratteri), spesso non reale
    re.compile(r"^(info|contact|support|sales|admin|office|hello|enquiries|marketing)$") # Valuta se escludere questi, a volte sono utili
    # Per il momento, teniamo i local part comuni come info, contact, ecc. commentati o non inclusi,
    # perché potrebbero essere validi per piccole aziende. Il filtro sul dominio è più importante.
]

class WebsiteRelevanceAnalyzer:
    def __init__(self):
        # Parole chiave relative al settore fotovoltaico e domotico
        self.fotovoltaico_keywords = [
            "fotovoltaico", "pannelli solari", "energia solare", "impianti solari", 
            "risparmio energetico", "autoconsumo", "accumulo energia", "inverter",
            "rinnovabile", "green energy", "impianto solare", "installazione pannelli",
            "manutenzione fotovoltaico", "kw", "kwh", "impianto elettrico", "ecobonus",
            "superbonus", "incentivi", "detrazione fiscale"
        ]
        
        self.domotica_keywords = [
            "domotica", "casa intelligente", "smart home", "automazione casa", 
            "sistema domotico", "controllo remoto", "building automation", 
            "illuminazione intelligente", "gestione energetica", "assistente vocale",
            "termostato smart", "alexa", "google home", "apple homekit",
            "cancelli automatici", "antifurto", "videosorveglianza"
        ]
        
        # Parole chiave generiche per installatori/tecnici
        self.installer_keywords = [
            "installazione", "installatore", "tecnico", "manutenzione", "progettazione", 
            "preventivo gratuito", "sopralluogo", "servizio clienti", "assistenza tecnica",
            "certificazione", "esperienza", "professionalità", "intervento", "montaggio"
        ]

        self.metalmeccanica_keywords = [
            "officina meccanica",
            "lavorazioni meccaniche",
            "lavorazioni cnc",
            "torneria",
            "fresatura cnc",
            "carpenteria metallica",
            "costruzioni meccaniche",
            "lavorazioni acciaio",
            "taglio laser metalli",
            "piegatura lamiera",
            "saldatura industriale",
            "meccanica di precisione"
        ]

        self.plastica_keywords = [
            "stampaggio plastica",
            "stampaggio materie plastiche",
            "iniezione plastica",
            "estrusione plastica",
            "lavorazioni plastica",
            "produzione articoli plastici",
            "stampaggio gomma",
            "termoformatura plastica"
        ]

        self.legno_keywords = [
            "falegnameria industriale",
            "produzione mobili",
            "lavorazioni legno",
            "arredamento su misura",
            "produzione arredamenti",
            "serramenti in legno",
            "infissi produzione",
            "mobilificio"
        ]

        self.alimentari_keywords = [
            "industria alimentare",
            "produzione alimentare",
            "pastificio",
            "caseificio",
            "salumificio",
            "produzione conserve",
            "lavorazione carni",
            "produzione dolciaria"    
        ]
        
        # Headers per simulare un browser normale
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def resolve_use_details():
        choice = input(
            "Usare Google Places Details? [y/n, invio=default]: "
        ).strip().lower()

        if choice in ("y", "yes"):
            return True
        if choice in ("n", "no"):
            return False

        # fallback automatico
        mode = os.environ.get("PLACES_DETAILS_MODE", "web").lower()
        return mode == "details"

    
    def normalize_url(self, url):
        """Normalizza l'URL aggiungendo il protocollo se necessario."""
        if not url:
            return ""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url
    
    def fetch_website_content(self, url):
        """Scarica il contenuto HTML del sito web."""
        if not url:
            return None
            
        try:
            url = self.normalize_url(url)
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                logger.warning(f"Impossibile accedere al sito {url}. Status code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante l'accesso a {url}: {e}")
            
            # Prova con http:// se https:// fallisce
            if url.startswith('https://'):
                try:
                    http_url = url.replace('https://', 'http://')
                    logger.info(f"Provo con protocollo HTTP: {http_url}")
                    response = requests.get(http_url, headers=self.headers, timeout=10)
                    if response.status_code == 200:
                        return response.text
                except:
                    pass
            return None
    
    def extract_domain(self, url):
        """Estrae il dominio principale dall'URL."""
        if not url:
            return ""
            
        try:
            parsed_url = urlparse(self.normalize_url(url))
            domain = parsed_url.netloc
            # Rimuovi www. se presente
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return url
    
    def extract_text_from_html(self, html):
        """Estrae il testo pulito dall'HTML."""
        if not html:
            return ""
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Rimuove script e stili
        for script_or_style in soup(['script', 'style', 'iframe', 'noscript']):
            script_or_style.decompose()
        
        # Estrae il testo
        text = soup.get_text(separator=' ', strip=True)
        
        # Normalizza spazi e righe
        text = re.sub(r'\s+', ' ', text)
        
        return text.lower()
    
    def extract_meta_info(self, html):
        """Estrae informazioni da meta tag, titolo e descrizione."""
        if not html:
            return ""
        
        soup = BeautifulSoup(html, 'html.parser')
        meta_info = []
        
        # Estrai il titolo
        if soup.title and soup.title.string:
            try:
                title_text = soup.title.string.strip()
                if title_text:
                    meta_info.append(title_text)
            except:
                pass
        
        # Estrai meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            try:
                content = meta_desc.get('content')
                if isinstance(content, str) and content.strip():
                    meta_info.append(content.strip())
            except:
                pass
        
        # Estrai meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            try:
                content = meta_keywords.get('content')
                if isinstance(content, str) and content.strip():
                    meta_info.append(content.strip())
            except:
                pass
        
        # Estrai h1, h2, h3 (intestazioni principali)
        for header in soup.find_all(['h1', 'h2', 'h3']):
            if header.text.strip():
                meta_info.append(header.text.strip())
        
        safe_parts = [part for part in meta_info if isinstance(part, str) and part]
        return ' '.join(safe_parts).lower()
    
    def analyze_website_relevance(self, url):
        """Analizza la pertinenza del sito web rispetto al settore fotovoltaico/domotico."""
        if not url:
            return {
                'is_relevant': False,
                'confidence': 0.0,
                'category': 'unknown',
                'reason': "URL non valido o mancante"
            }
            
        logger.info(f"Analisi pertinenza del sito: {url}")
        
        # Estrai il dominio per controlli immediati
        domain = self.extract_domain(url)
        
        # Controlla se il dominio contiene parole chiave evidenti
        domain_keywords = re.findall(r'([a-zA-Z]+)', domain)
        domain_text = ' '.join(domain_keywords).lower()
        
        # Controllo immediato sul dominio
        fotovoltaico_in_domain = any(kw in domain_text for kw in ['fotovoltaic', 'solar', 'energi', 'pannell'])
        domotica_in_domain = any(kw in domain_text for kw in ['domotica', 'smart', 'home', 'automaz'])
        
        if fotovoltaico_in_domain or domotica_in_domain:
            logger.info(f"Rilevanza immediata dal dominio: {domain}")
            return {
                'is_relevant': True,
                'confidence': 0.8,
                'category': 'fotovoltaico' if fotovoltaico_in_domain else 'domotica',
                'reason': f"Parole chiave rilevanti nel dominio: {domain}"
            }
        
        # Scarica il contenuto
        html_content = self.fetch_website_content(url)
        if not html_content:
            return {
                'is_relevant': False, 
                'confidence': 0.5,
                'category': 'unknown',
                'reason': "Impossibile accedere al sito web"
            }
        
        # Estrai testo dal sito
        full_text = self.extract_text_from_html(html_content)
        meta_text = self.extract_meta_info(html_content)
        
        # Combinazione con peso maggiore per meta info
        weighted_text = meta_text + " " + meta_text + " " + full_text
        
        # Conta le occorrenze delle parole chiave
        fotovoltaico_matches = sum(weighted_text.count(kw) for kw in self.fotovoltaico_keywords)
        domotica_matches = sum(weighted_text.count(kw) for kw in self.domotica_keywords)
        installer_matches = sum(weighted_text.count(kw) for kw in self.installer_keywords)
        
        # Normalizza in base alla lunghezza del testo (per siti con molto contenuto)
        text_length_factor = min(1.0, 2000 / max(len(weighted_text), 500))
        fotovoltaico_score = fotovoltaico_matches * text_length_factor
        domotica_score = domotica_matches * text_length_factor
        installer_score = installer_matches * text_length_factor
        
        # Calcola il punteggio totale
        primary_score = max(fotovoltaico_score, domotica_score)
        total_score = primary_score + (installer_score * 0.5)  # Installer keywords hanno peso minore
        
        # Imposta soglie di pertinenza
        is_relevant = total_score >= 3.0
        category = 'fotovoltaico' if fotovoltaico_score >= domotica_score else 'domotica'
        
        # Calcola la confidenza (0.5-1.0)
        confidence = min(1.0, max(0.5, 0.5 + (total_score / 20)))
        
        result = {
            'is_relevant': is_relevant,
            'confidence': round(confidence, 2),
            'category': category if is_relevant else 'non_pertinente',
            'scores': {
                'fotovoltaico': round(fotovoltaico_score, 2),
                'domotica': round(domotica_score, 2),
                'installer': round(installer_score, 2),
                'total': round(total_score, 2)
            },
            'reason': self.generate_reason(is_relevant, fotovoltaico_score, domotica_score, installer_score)
        }
        
        logger.info(f"Analisi completata per {url}: {result['is_relevant']} ({result['category']}, {result['confidence']})")
        return result
    
    def generate_reason(self, is_relevant, fotovoltaico_score, domotica_score, installer_score):
        """Genera una spiegazione della decisione."""
        if not is_relevant:
            return "Contenuto insufficiente relativo al settore fotovoltaico o domotico"
        
        main_category = "fotovoltaico" if fotovoltaico_score >= domotica_score else "domotica"
        main_score = max(fotovoltaico_score, domotica_score)
        
        reason = f"Rilevato contenuto pertinente al settore {main_category} "
        
        if installer_score > 2:
            reason += f"con indicazioni di servizi di installazione"
        elif main_score > 8:
            reason += f"con alto numero di riferimenti specifici"
        else:
            reason += f"con riferimenti sufficienti"
            
        return reason

def load_comuni(file_path):
    """Carica la lista dei comuni da un file CSV/Excel"""
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)
    
    # Assumiamo che ci sia una colonna 'comune' nel file
    return df['comune'].tolist() if 'comune' in df.columns else df.iloc[:, 0].tolist()

def _load_processed_comuni():
    """Carica la lista dei comuni già elaborati dal log"""
    processed_file = "logs/comuni_elaborati.csv"
    if not os.path.exists(processed_file):
        return set()
    
    try:
        df = pd.read_csv(processed_file)
        return set(df['comune'].tolist()) if 'comune' in df.columns else set()
    except Exception as e:
        logging.warning(f"Errore nel caricamento comuni elaborati: {e}")
        return set()

def _save_processed_comuni(comuni_list):
    """Salva i comuni elaborati nel log"""
    processed_file = "logs/comuni_elaborati.csv"
    
    # Crea la directory logs se non esiste
    os.makedirs("logs", exist_ok=True)
    
    # Carica i comuni già elaborati
    existing_comuni = _load_processed_comuni()
    
    # Aggiungi i nuovi comuni
    all_comuni = existing_comuni.union(set(comuni_list))
    
    # Salva nel file
    df = pd.DataFrame({'comune': sorted(list(all_comuni))})
    df.to_csv(processed_file, index=False)
    logging.info(f"Salvati {len(comuni_list)} nuovi comuni nel log. Totale: {len(all_comuni)}")

def filter_and_limit_comuni(comuni_list, max_comuni=5):
    """Filtra i comuni già elaborati e limita a max_comuni per esecuzione"""
    processed_comuni = _load_processed_comuni()
    
    # Filtra i comuni non ancora elaborati
    unprocessed_comuni = [c for c in comuni_list if c not in processed_comuni]
    
    logging.info(f"Comuni totali: {len(comuni_list)}")
    logging.info(f"Comuni già elaborati: {len(processed_comuni)}")
    logging.info(f"Comuni da elaborare: {len(unprocessed_comuni)}")
    
    # Limita a max_comuni
    selected_comuni = unprocessed_comuni[:max_comuni]
    
    if len(selected_comuni) < len(unprocessed_comuni):
        logging.info(f"Limitando elaborazione a {max_comuni} comuni per questa esecuzione")
        logging.info(f"Comuni rimanenti: {len(unprocessed_comuni) - max_comuni}")
    
    return selected_comuni

# Funzione per ottenere coordinate e raggio appropriato per un comune
def get_comune_coordinates(comune_name):
    """Ottiene le coordinate geografiche di un comune utilizzando OpenStreetMap Nominatim API"""
    try:
        # Aggiungi "Italia" per limitare la ricerca all'Italia
        search_query = f"{comune_name}, Italia"
        
        # Formatta la query per URL
        encoded_query = quote_plus(search_query)
        
        # URL dell'API Nominatim (OpenStreetMap)
        url = f"https://nominatim.openstreetmap.org/search?q={encoded_query}&format=json&limit=1"
        
        # Headers per rispettare i termini di servizio di Nominatim
        headers = {
            'User-Agent': 'GoogleMapsScraper/1.0',
            'Accept': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        data = response.json()
        
        if data and len(data) > 0:
            # Estrai latitudine e longitudine
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            
            # Estrai anche il bounding box per determinare il raggio appropriato
            if 'boundingbox' in data[0]:
                bbox = data[0]['boundingbox']
                # Calcola una stima approssimativa delle dimensioni del comune
                lat_diff = abs(float(bbox[1]) - float(bbox[0]))
                lon_diff = abs(float(bbox[3]) - float(bbox[2]))
                
                # Stima del raggio in km (approssimativo)
                earth_radius = 6371  # km
                lat_km = lat_diff * (math.pi/180) * earth_radius
                lon_km = lon_diff * (math.pi/180) * earth_radius * math.cos(lat * math.pi/180)
                radius = max(lat_km, lon_km) / 2
                
                # Imposta un raggio minimo di 2 km e massimo di 10 km
                radius = max(2, min(radius, 10))
            else:
                # Raggio predefinito se non è disponibile il bounding box
                radius = 5  # km
                
            # Aggiungi anche il display_name per verifica
            display_name = data[0].get('display_name', '')
            
            return {
                'lat': lat,
                'lon': lon,
                'radius': radius,  # Raggio in km
                'display_name': display_name
            }
        else:
            logging.warning(f"Coordinate non trovate per il comune: {comune_name}")
            return None
            
    except Exception as e:
        logging.error(f"Errore nel recupero delle coordinate per {comune_name}: {str(e)}")
        return None

# Crea un dizionario di coordinate per tutti i comuni per evitare chiamate ripetute
def build_comune_coordinates_dict(comuni):
    """Crea un dizionario di coordinate per tutti i comuni"""
    coordinates_dict = {}
    total_comuni = len(comuni)
    
    for i, comune in enumerate(comuni):
        logging.info(f"Recupero coordinate per {comune} ({i+1}/{total_comuni})")
        coordinates = get_comune_coordinates(comune)
        
        if coordinates:
            coordinates_dict[comune] = coordinates
            logging.info(f"Coordinate trovate per {comune}: {coordinates['lat']}, {coordinates['lon']}, " 
                         f"raggio: {coordinates['radius']} km")
        else:
            logging.warning(f"Impossibile trovare coordinate per {comune}")
        
        # Rispetta i limiti di utilizzo dell'API
        time.sleep(1.5)
    
    return coordinates_dict

# Versione migliorata di generate_search_urls con coordinate
def generate_search_urls_with_coordinates(comuni, keywords, coordinates_dict):
    """Genera URL di ricerca per Google Maps combinando comuni, keywords e coordinate geografiche"""
    urls = []
    
    # Filtra i comuni per i quali abbiamo le coordinate
    comuni_with_coords = [comune for comune in comuni if comune in coordinates_dict]
    
    for comune in comuni_with_coords:
        coords = coordinates_dict[comune]
        lat, lon = coords['lat'], coords['lon']
        radius = coords['radius']  # Raggio in km
        
        for keyword in keywords:
            search_term = f"{keyword} {comune}"
            encoded_term = quote_plus(search_term)
            
            # URL con coordinate e raggio specifico
            # Format: @lat,lon,zoom where zoom is calculated based on radius
            # Zoom levels: 20 (building), 15 (street), 13 (small area), 10 (city), 5 (region)
            zoom = max(13 - int(radius/3), 10)  # Calcola lo zoom in base al raggio
            
            url = f"https://www.google.com/maps/search/{encoded_term}/@{lat},{lon},{zoom}z"
            
            urls.append({
                "comune": comune, 
                "keyword": keyword, 
                "url": url,
                "lat": lat,
                "lon": lon,
                "radius": radius
            })
    
    return urls

def scrape_with_serpapi_geo(search_urls, api_key):
    """Scrape dei risultati utilizzando SerpAPI con parametri di geolocalizzazione precisi"""
    results = []
    
    for search in search_urls:
        comune = search['comune']
        keyword = search['keyword']
        lat = search['lat']
        lon = search['lon']
        radius = search['radius']
        
        logging.info(f"Cercando: {keyword} in {comune} (lat: {lat}, lon: {lon}, raggio: {radius}km)")
        
        params = {
            "engine": "google_maps",
            "q": f"{keyword}",  # Usa solo la keyword, non il comune
            "api_key": api_key,
            "type": "search",
            "ll": f"@{lat},{lon},{radius}z",  # Localizzazione specifica con zoom
            "lsig": "AB86z5U7ciOzWK6VuWq1jrX9aCNj",  # Parametro che aiuta a focalizzare i risultati
            "no_cache": "true"  # Assicura risultati aggiornati
        }
        
        try:
            response = requests.get("https://serpapi.com/search", params=params)
            data = response.json()
            
            if "local_results" in data and data["local_results"]:
                for place in data["local_results"]:
                    # Verifica che il risultato sia effettivamente nel comune specificato
                    address = place.get("address", "")
                    
                    # Verifica se l'indirizzo contiene il nome del comune
                    # Questo è un controllo aggiuntivo per filtrare risultati fuori area
                    if comune.lower() in address.lower() or comune.lower() in place.get("title", "").lower():
                        result = {
                            "comune": comune,
                            "keyword": keyword,
                            "nome": place.get("title", ""),
                            "indirizzo": address,
                            "telefono": place.get("phone", ""),
                            "sito_web": place.get("website", ""),
                            "num_recensioni": place.get("reviews", ""),
                            "tipo": place.get("type", ""),
                            "email": "",
                            "linkedin": "",
                            "pertinenza": 
                            "categoria": "",
                            "confidenza_analisi": 0.0,
                            "distanza_km": place.get("distance", "")
                        }
                        results.append(result)
                    else:
                        logging.info(f"Risultato escluso perché fuori dal comune: {place.get('title')}, {address}")
            
            # Rispettiamo i rate limit di SerpAPI
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Errore durante lo scraping di {keyword} {comune}: {str(e)}")
    
    return results

def scrape_with_places_api(search_urls, api_key, fetch_details=True, per_query_limit=None):
    """Recupera risultati aziende usando Google Places API (Nearby Search + Details).

    Parametri
    - search_urls: lista di dict con chiavi: comune, keyword, lat, lon, radius (km)
    - api_key: chiave API di Google Places
    - fetch_details: se True richiama anche Place Details per sito e telefono
    - per_query_limit: se impostato, limita i risultati per coppia (comune, keyword)
    """
    base_nearby_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    base_details_url = "https://maps.googleapis.com/maps/api/place/details/json"

    session = requests.Session()
    session.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'OutreachSaaS/1.0'
    })

    aggregated_results = []
    counters = {"nearby_requests": 0, "details_requests": 0}

    # Cache Place Details (website) per place_id per ridurre spesa
    details_cache_path = os.path.join("data", "cache", "places_details_cache.json")
    _ensure_parent_dir = lambda p: os.makedirs(os.path.dirname(p), exist_ok=True)
    def _load_details_cache():
        try:
            if os.path.exists(details_cache_path):
                with open(details_cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logging.debug(f"[PlacesAPI] Impossibile caricare cache Details: {e}")
        return {}
    def _save_details_cache(cache_obj):
        try:
            _ensure_parent_dir(details_cache_path)
            with open(details_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.debug(f"[PlacesAPI] Impossibile salvare cache Details: {e}")

    def _is_likely_relevant(name: str, types: list, rating: float, user_ratings_total: int) -> bool:
        try:
            n = (name or "").lower()
            t = [x.lower() for x in (types or [])]
            # Whitelist tipi affini
            whitelist_types = {
                'solar_energy_equipment_supplier', 'electrician', 'hvac_contractor',
                'general_contractor', 'contractor', 'solar_energy_company'
            }
            if any(w in t for w in whitelist_types):
                pass_ok_type = True
            else:
                pass_ok_type = False
            # Keyword nel nome
            kw_ok = any(k in n for k in [
                'fotovolta', 'solare', 'solar', 'energie rinnovabili', 'impianti elettr', 'elettric'
            ])
            # Blacklist banale per ridurre falsi positivi
            blacklist = ['hotel', 'ristor', 'parking', 'farmacia', 'bar ', 'autolav', 'viaggi', 'parrucch', 'estetic']
            if any(b in n for b in blacklist):
                return False
            # Soglia minima serietà (facoltativa)
            serious_ok = (user_ratings_total or 0) >= 2 or (rating or 0) >= 3.5
            return (pass_ok_type or kw_ok) and serious_ok
        except Exception:
            return False

    details_cache = _load_details_cache()
    cache_dirty = False

    for search in search_urls:
        comune = search.get('comune', '')
        keyword = search.get('keyword', '')
        lat = search.get('lat')
        lon = search.get('lon')
        radius_km = search.get('radius', 5)
        radius_m = int(max(1, float(radius_km)) * 1000)

        logging.info(f"[PlacesAPI] Cercando: '{keyword}' in {comune} (lat: {lat}, lon: {lon}, raggio: {radius_m} m)")

        params = {
            'key': api_key,
            'location': f"{lat},{lon}",
            'radius': radius_m,
            'keyword': keyword,
            'opennow': False,
        }

        fetched_for_query = 0
        page_token = None

        for page_index in range(3):
            if page_token:
                params['pagetoken'] = page_token
                time.sleep(2.0)

            try:
                resp = session.get(base_nearby_url, params=params, timeout=20)
                counters["nearby_requests"] += 1
                data = resp.json()
            except Exception as e:
                logging.error(f"[PlacesAPI] Errore richiesta Nearby Search per {keyword} {comune}: {e}")
                break

            status = data.get('status')
            if status not in {"OK", "ZERO_RESULTS"}:
                logging.warning(f"[PlacesAPI] Stato non OK: {status} - messaggio: {data.get('error_message', '')}")
                break

            results = data.get('results', [])
            if not results:
                break

            seen_place_ids = set()
            for idx, place in enumerate(results):
                if per_query_limit is not None and fetched_for_query >= per_query_limit:
                    break

                place_id = place.get('place_id')
                name = place.get('name', '')
                address = place.get('vicinity') or place.get('formatted_address', '')
                user_ratings_total = place.get('user_ratings_total', 0)
                types = place.get('types', [])
                distance_km = ""

                website = ""
                phone = ""

                # Evita duplicati per stesso place_id nella stessa esecuzione
                if place_id and place_id in seen_place_ids:
                    continue
                if place_id:
                    seen_place_ids.add(place_id)

                # Decide se fare Details (quando attivo) - se fetch_details=True, chiama per tutti
                if fetch_details and place_id:
                    # Cache: se già abbiamo il website, usa quello
                    cached = details_cache.get(place_id, {})
                    website = cached.get('website', '') or website
                    if not website:
                        details_params = {
                            'key': api_key,
                            'place_id': place_id,
                            'fields': 'website'  # only website per ridurre quota
                        }
                        try:
                            det_resp = session.get(base_details_url, params=details_params, timeout=20)
                            counters["details_requests"] += 1
                            det_data = det_resp.json()
                            if det_data.get('status') == 'OK':
                                det_res = det_data.get('result', {})
                                website = det_res.get('website', '') or website
                                if website:
                                    details_cache[place_id] = {'website': website}
                                    cache_dirty = True
                                    logging.info(f"[PlacesAPI] Sito via Details per {name} ({comune}): {website}")
                            else:
                                logging.debug(f"[PlacesAPI] Details non OK per {name}: {det_data.get('status')} {det_data.get('error_message', '')}")
                        except Exception as e:
                            logging.debug(f"[PlacesAPI] Errore Details per {name}: {e}")
                elif not fetch_details:
                    # Solo se Details è disabilitato: per i top-N prova piccola web search per il sito ufficiale
                    try:
                        if idx < 10 and name:
                            logging.info(f"[PlacesAPI] Provo WebSearch per sito: {name} ({comune}) [rank {idx+1}]")
                            site_guess = _find_official_site_via_web(name, comune)
                            if site_guess:
                                website = site_guess
                                logging.info(f"[PlacesAPI] Sito stimato via WebSearch per {name} ({comune}): {website}")
                            else:
                                logging.info(f"[PlacesAPI] WebSearch nessun sito per {name} ({comune})")
                        elif name:
                            logging.debug(f"[PlacesAPI] Skipping WebSearch per {name} ({comune}) oltre top-10 (idx={idx})")
                    except Exception as e:
                        logging.debug(f"[PlacesAPI] Web search sito fallita per {name}: {e}")
                else:
                    # Se Details è attivo ma non ha trovato il sito, non fare web search
                    if name and not website:
                        logging.info(f"[PlacesAPI] Details attivo ma nessun sito trovato per {name} ({comune})")

                mapped = {
                    "comune": comune,
                    "keyword": keyword,
                    "nome": name,
                    "indirizzo": address or "",
                    "telefono": phone,
                    "sito_web": website,
                    "num_recensioni": user_ratings_total,
                    "tipo": ",".join(types) if types else "",
                    "email": "",
                    "linkedin": "",
                    "pertinenza": False,
                    "categoria": "",
                    "confidenza_analisi": 0.0,
                    "distanza_km": distance_km
                }

                if website:
                    logging.info(f"[PlacesAPI] Mapping finale: {name} | {comune} -> sito_web={website}")
                aggregated_results.append(mapped)
                fetched_for_query += 1

            if per_query_limit is not None and fetched_for_query >= per_query_limit:
                break

            page_token = data.get('next_page_token')
            if not page_token:
                break

            time.sleep(1.0)

        time.sleep(random.uniform(0.5, 1.2))

    # Salva cache se aggiornata
    if cache_dirty:
        _save_details_cache(details_cache)

    return aggregated_results, counters

def _find_official_site_via_web(company_name: str, comune: str) -> str:
    """Esegue una web search leggera per trovare il sito ufficiale di un'azienda.
    Ritorna l'URL normalizzato del dominio se sembra plausibile, altrimenti stringa vuota.
    """
    try:
        query = f"{company_name} {comune} sito ufficiale"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }
        from urllib.parse import quote_plus, parse_qs
        def is_bad(domain: str) -> bool:
            d = domain.lower()
            bad = [
                'google.', 'gstatic.com', 'googleusercontent.com', 'maps.googleapis.', 'support.google.', 'policies.google.',
                'facebook.com', 'instagram.com', 'linkedin.com', 'paginegialle', 'tripadvisor', 'youtube.com', 'tiktok.com',
                'amazon.', 'ebay.', 'subito.', 'wikipedia.org'
            ]
            return any(b in d for b in bad)

        # 1) Google SERP (gestisce anche /url?q=)
        url_g = f"https://www.google.com/search?q={quote_plus(query)}&hl=it"
        r = requests.get(url_g, headers=headers, timeout=10)
        candidates = []
        if r.status_code == 200 and r.text:
            soup = BeautifulSoup(r.text, 'html.parser')
            seen = set()
            # a) Link diretti
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

        # 2) DuckDuckGo HTML fallback se Google non ha reso risultati utilizzabili
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

        # 3) Heuristic fallback: prova uno slug dominio dal nome
        try:
            base = re.sub(r'[^a-z0-9]+', '', (company_name or '').lower())
            guess = f"https://www.{base}.it"
            g = requests.head(guess, headers=headers, timeout=5, allow_redirects=True)
            if 200 <= g.status_code < 400:
                return clean_url(guess)
        except:
            pass
        return ""
    except Exception as e:
        logging.debug(f"_find_official_site_via_web error: {e}")
        return ""

def _append_api_usage_log(counters, log_dir="logs", log_filename="places_api_usage.csv"):
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_path = os.path.join(log_dir, log_filename)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        total = int(counters.get("nearby_requests", 0)) + int(counters.get("details_requests", 0))

        # Compute cumulative total
        cumulative_total = total
        if os.path.exists(log_path):
            try:
                with open(log_path, mode='r', encoding='utf-8', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            cumulative_total += int(row.get('total_requests', 0))
                        except:
                            continue
            except Exception as e:
                logger.warning(f"Impossibile leggere log esistente per calcolare il cumulativo: {e}")

        file_exists = os.path.exists(log_path)
        with open(log_path, mode='a', encoding='utf-8', newline='') as f:
            fieldnames = [
                'timestamp', 'nearby_requests', 'details_requests', 'total_requests', 'cumulative_total_requests'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'timestamp': timestamp,
                'nearby_requests': counters.get('nearby_requests', 0),
                'details_requests': counters.get('details_requests', 0),
                'total_requests': total,
                'cumulative_total_requests': cumulative_total
            })
        logger.info(
            f"[PlacesAPI] Richieste questa esecuzione - nearby: {counters.get('nearby_requests',0)}, details: {counters.get('details_requests',0)}, totale: {total}. Cumulativo: {cumulative_total}"
        )
    except Exception as e:
        logger.error(f"Errore nella scrittura del log di utilizzo Places API: {e}")

def scrape_with_selenium(search_urls, coordinates_dict):
    """Scrape dei risultati utilizzando Selenium (alternativa a SerpAPI)"""
    results = []
    
    # Configurazione di Chrome con impostazioni più stealth
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    # Inizializza il webdriver
    driver = webdriver.Chrome(service=webdriver.ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    for search in search_urls:
        comune_attuale = search['comune']
        keyword = search['keyword']
        url = search['url']
        
        logger.info(f"Cercando: {keyword} in {comune_attuale}")
        
        try:
            driver.get(url)
            
            # Screenshot iniziale per debug
            screenshot_filename = f"initial_{comune_attuale}_{keyword}.png".replace(" ", "_")
            debug_path = os.path.join(SCREENSHOT_DIR, screenshot_filename)
            driver.save_screenshot(debug_path)
            logger.info(f"Screenshot iniziale salvato: {debug_path}")
            
            # Gestione consenso cookie con selettori specifici e multipli approcci
            cookie_selectors = [
                (By.ID, "L2AGLb"),  # Pulsante "Accetta tutto" tramite ID
                (By.CSS_SELECTOR, ".tHlp8d"),  # Classe specifica del pulsante
                (By.CSS_SELECTOR, "button[aria-label='Accetta tutto']"),
                (By.CSS_SELECTOR, "button[aria-label='Accept all']"),
                (By.XPATH, "//button[contains(text(), 'Accetta tutto')]"),
                (By.XPATH, "//button[contains(text(), 'Accept all')]"),
                (By.XPATH, "//div[@role='dialog']//button[contains(., 'Accetta')]"),
                (By.XPATH, "//div[@role='dialog']//button[contains(., 'Accept')]")
            ]
            
            # Prova tutti i selettori con timeout breve
            for selector_type, selector in cookie_selectors:
                try:
                    consent_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((selector_type, selector))
                    )
                    logger.info(f"Trovato pulsante consenso con selettore: {selector}")
                    driver.execute_script("arguments[0].click();", consent_button)
                    time.sleep(2)  # Attesa per completare l'azione
                    logger.info("Pulsante consenso cliccato con successo")
                    
                    # Screenshot dopo il click
                    screenshot_filename = f"after_consent_{comune_attuale}_{keyword}.png".replace(" ", "_")
                    driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
                    break
                except Exception as e:
                    continue
            
            # Attesa generale per il caricamento completo
            time.sleep(5)
            
            # Scrolla lentamente nella pagina per caricare più risultati
            logger.info("Scrolling per caricare risultati...")
            for i in range(7):  # Più scrolling rispetto alla versione precedente
                driver.execute_script(f"window.scrollBy(0, {300 + i*100});")  # Scrolling progressivo
                time.sleep(1.5)  # Pausa più breve tra scrolling
            
            # Screenshot dopo scrolling
            screenshot_filename = f"after_scroll_{comune_attuale}_{keyword}.png".replace(" ", "_")
            driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
            
            # Selettori aggiornati per risultati Google Maps 2023/2024
            selectors_to_try = [
                "div[role='article']",
                "div.Nv2PK", 
                "a[href^='/maps/place']",
                "div.section-result",
                "div.bfdHYd",  # Classe dei risultati aggiornata
                "div.V0h1Ob-haAclf",  # Classe alternativa dei risultati
                "div.DxyBCb"  # Ulteriore classe di risultati
            ]
            
            result_elements = []
            used_selector = ""
            
            for selector in selectors_to_try:
                try:
                    temp_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if temp_elements and len(temp_elements) > 0:
                        result_elements = temp_elements
                        used_selector = selector
                        logger.info(f"Trovati {len(result_elements)} risultati usando il selettore: {selector}")
                        break
                except Exception as e:
                    logger.warning(f"Errore con selettore {selector}: {str(e)}")
            
            if not result_elements:
                logger.warning(f"Nessun risultato trovato per {keyword} {comune_attuale}")
                screenshot_filename = f"no_results_{comune_attuale}_{keyword}.png".replace(" ", "_")
                driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
                continue
            
            # Processa ogni risultato trovato
            for i in range(min(10, len(result_elements))):  # Limita a 10 risultati per ricerca
                try:
                    logger.info(f"Elaborazione risultato {i+1}/{min(10, len(result_elements))}")
                    
                    # Importante: recupera l'elemento ATTUALE
                    # Questo evita il problema di "stale element"
                    result_elements = driver.find_elements(By.CSS_SELECTOR, used_selector)
                    
                    # Verifica che ci siano abbastanza elementi
                    if i >= len(result_elements):
                        logger.warning(f"Indice {i} fuori limite. Totale elementi: {len(result_elements)}")
                        break
                        
                    # Prendi l'elemento attuale
                    element = result_elements[i]
                    
                    # Scorri fino all'elemento per assicurarsi che sia visibile
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                    time.sleep(1)
                    
                    # Estrai nome usando selettori multipli e JavaScript
                    name = ""
                    name_selectors = ["h3", ".qBF1Pd", ".fontHeadlineSmall", "[jsan*='fontHeadlineSmall']", 
                                    ".section-result-title", "span.OSrXXb", "[jstcache]", "[class*='title']"]
                    
                    for ns in name_selectors:
                        try:
                            name_elements = element.find_elements(By.CSS_SELECTOR, ns)
                            if name_elements:
                                name = name_elements[0].text.strip()
                                if name:
                                    break
                        except:
                            continue
                    
                    # Fallback con JavaScript per estrarre il nome
                    if not name:
                        try:
                            name = driver.execute_script("""
                                var el = arguments[0];
                                var headers = el.querySelectorAll('h1, h2, h3, h4, h5, .fontHeadlineSmall, [class*="title"], [class*="name"]');
                                if (headers && headers.length > 0) return headers[0].innerText;
                                return el.innerText.split('\\n')[0]; // Fallback: prima riga di testo
                            """, element)
                        except:
                            pass
                    
                    # Se ancora non abbiamo un nome, ottieni aria-label
                    if not name or name == "":
                        try:
                            name = element.get_attribute("aria-label")
                        except:
                            pass
                            
                    # Se ancora non abbiamo un nome valido, saltiamo
                    if not name or name == "":
                        logger.warning("Nome non trovato, risultato saltato")
                        continue
                            
                    # Click sull'elemento per aprire i dettagli
                    logger.info(f"Apertura dettagli per: {name}")
                    
                    # Salva lo stato attuale (URL) per poter tornare indietro con precisione
                    current_list_page_url = driver.current_url # Rinominato per chiarezza
                    
                    # Assicurati che l'elemento sia visibile
                    try:
                        # Scorri fino all'elemento
                        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", element)
                        time.sleep(2)  # Attesa per lo scroll

                        # Prova diversi metodi per il click, iniziando dal click diretto
                        click_methods = [
                            # Metodo 1: Click diretto (ora è il primo metodo)
                            lambda: element.click(),
                            # Metodo 2: Click con Actions
                            lambda: webdriver.ActionChains(driver).move_to_element(element).click().perform(),
                            # Metodo 3: Click con JavaScript
                            lambda: driver.execute_script("arguments[0].click();", element),
                            # Metodo 4: Click con JavaScript alternativo
                            lambda: driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {bubbles: true}));", element)
                        ]

                        success = False
                        for click_method in click_methods:
                            try:
                                click_method()
                                time.sleep(3)  # Attesa per il caricamento
                                
                                # Verifica se siamo nella pagina dettagli
                                if "/maps/place/" in driver.current_url:
                                    success = True
                                    logger.info("Pagina dettagli aperta con successo")
                                    break
                                else:
                                    # Se l'URL non è cambiato, prova il prossimo metodo
                                    logger.warning("Click non riuscito, provo un altro metodo...")
                                    
                            except Exception as e:
                                logger.warning(f"Metodo di click fallito: {str(e)}")
                                continue

                        if not success:
                            logger.error(f"Impossibile aprire i dettagli per: {name}")
                            # Salva screenshot per debug
                            screenshot_filename = f"click_failed_{name}.png".replace(" ", "_")
                            driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
                            # Tentativo di tornare alla pagina elenco prima di continuare
                            try:
                                driver.get(current_list_page_url)
                                time.sleep(2)
                            except:
                                driver.get(url) # Fallback all'URL di ricerca originale
                                time.sleep(3)
                            continue

                    except Exception as e:
                        logger.error(f"Errore durante l'apertura dei dettagli: {str(e)}")
                        # Tentativo di tornare alla pagina elenco prima di continuare
                        try:
                            driver.get(current_list_page_url)
                            time.sleep(2)
                        except:
                            driver.get(url) # Fallback all'URL di ricerca originale
                            time.sleep(3)
                        continue
                    
                    # --- Estrazione dei dettagli ---
                    
                    # Estrai lat e lon dall'URL della pagina dei dettagli (se presenti)
                    elemento_lat, elemento_lon = None, None
                    try:
                        # Esempio URL: https://www.google.com/maps/place/NOME/@LAT,LON,ZOOMz/...
                        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", driver.current_url)
                        if match:
                            elemento_lat = match.group(1)
                            elemento_lon = match.group(2)
                            logger.info(f"Coordinate estratte per {name}: lat={elemento_lat}, lon={elemento_lon}")
                        else:
                            logger.info(f"Coordinate non trovate nell'URL per {name}: {driver.current_url}")
                    except Exception as e_coord:
                        logger.warning(f"Impossibile estrarre coordinate dall'URL per {name}: {e_coord}")
                    
                    # 1. Per l'estrazione dell'indirizzo:
                    address = ""
                    address_selectors = [
                        "button[data-item-id='address']",
                        "button[aria-label*='indirizzo']", 
                        "button[aria-label*='address']",
                        "button[data-tooltip*='indirizzo']",
                        "[data-item-id*='address']",
                        ".rogA2c",
                        ".fontBodyMedium"
                    ]

                    for selector in address_selectors:
                        try:
                            addr_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                            if addr_elements:
                                for ae in addr_elements:
                                    addr_text = ae.text.strip() or ae.get_attribute("aria-label")
                                    if addr_text:
                                        # Applica la pulizia migliorata
                                        address = clean_extracted_text(addr_text)
                                        break
                                if address:
                                    break
                        except:
                            continue

                    # 2. Per l'estrazione del telefono:
                    phone = ""
                    phone_selectors = [
                        "button[data-item-id='phone:tel']",
                        "button[aria-label*='telefono']",
                        "button[aria-label*='phone']",
                        "button[data-tooltip*='telefono']",
                        "[data-item-id*='phone']",
                        "button[aria-label*='call']",
                        ".rogA2c"
                    ]

                    for selector in phone_selectors:
                        try:
                            phone_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                            if phone_elements:
                                for pe in phone_elements:
                                    phone_text = pe.text.strip() or pe.get_attribute("aria-label")
                                    if phone_text:
                                        # Applica la pulizia migliorata
                                        phone = clean_extracted_text(phone_text)
                                        # Verifica che contenga numeri
                                        if re.search(r'\d', phone):
                                            break
                                if phone and re.search(r'\d', phone):
                                    break
                        except:
                            continue
                            
                    # 3. Estrai sito web
                    website = ""
                    website_selectors = [
                        "a[data-item-id='authority']",
                        "button[aria-label*='sito']",
                        "button[aria-label*='site']",
                        "button[aria-label*='website']",
                        "button[data-item-id*='website']",
                        "a[href*='http']",
                        "button[jsaction*='website']",
                        ".rogA2c"
                    ]
                    
                    for selector in website_selectors:
                        try:
                            web_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                            if web_elements:
                                for we in web_elements:
                                    # Prova a ottenere l'URL direttamente
                                    site_url = we.get_attribute("href")
                                    
                                    # Se non è un URL diretto, prova con il testo o aria-label
                                    if not site_url or not site_url.startswith(("http://", "https://")):
                                        web_text = we.text.strip() or we.get_attribute("aria-label")
                                        if web_text and ("sito web:" in web_text.lower() or "website:" in web_text.lower()):
                                            site_match = re.search(r'https?://[^\s"\']+', web_text)
                                            if site_match:
                                                site_url = site_match.group(0)
                                                
                                    # Se abbiamo un URL valido, salvalo
                                    if site_url and site_url.startswith(("http://", "https://")):
                                        website = site_url
                                        break
                                
                                if website:
                                    break
                        except:
                            continue
                    
                    # Crea il risultato
                    result = {
                        "comune": comune_attuale,
                        "keyword": keyword,
                        "nome": name,
                        "indirizzo": address,
                        "telefono": phone,
                        "sito_web": website,
                        "num_recensioni": "",
                        "tipo": "",
                        "email": "",
                        "linkedin": "",
                        "pertinenza": False,
                        "categoria": "",
                        "confidenza_analisi": 0.0
                    }
                    
                    # Verifica geolocalizzazione prima di salvare
                    salvare_risultato = False
                    motivazione_geo = "Nessuna condizione soddisfatta" # Default

                    if address:
                        address_lower = address.lower()
                        if comune_attuale.lower() in address_lower:
                            salvare_risultato = True
                            motivazione_geo = f"Comune '{comune_attuale}' trovato nell'indirizzo."
                            logger.info(f"Risultato '{name}' confermato nel comune '{comune_attuale}' tramite indirizzo.")
                        elif elemento_lat and elemento_lon and comune_attuale in coordinates_dict:
                            try:
                                if is_within_comune_boundaries(
                                    float(elemento_lat),
                                    float(elemento_lon),
                                    coordinates_dict[comune_attuale]
                                ):
                                    salvare_risultato = True
                                    motivazione_geo = f"Entro i confini di '{comune_attuale}' tramite coordinate (Lat: {elemento_lat}, Lon: {elemento_lon})."
                                    logger.info(f"Risultato '{name}' confermato nel comune '{comune_attuale}' tramite coordinate.")
                                else:
                                    motivazione_geo = f"FUORI dai confini di '{comune_attuale}' tramite coordinate (Lat: {elemento_lat}, Lon: {elemento_lon}, Indirizzo: {address}). Raggio comune: {coordinates_dict[comune_attuale].get('radius')}km."
                                    logger.info(f"Risultato '{name}' ({address}) escluso: fuori dai confini del comune '{comune_attuale}' in base alle coordinate.")
                            except ValueError:
                                motivazione_geo = f"Errore conversione coordinate per '{name}': lat={elemento_lat}, lon={elemento_lon}. Escluso."
                                logger.warning(motivazione_geo)
                            except KeyError:
                                motivazione_geo = f"Coordinate per '{comune_attuale}' non in coordinates_dict. Escluso '{name}'."
                                logger.warning(motivazione_geo)
                        else:
                            motivazione_geo = f"Indirizzo non contiene '{comune_attuale}' e controllo coordinate non possibile/fallito per '{name}' ({address})."
                            logger.info(motivazione_geo)
                    elif elemento_lat and elemento_lon and comune_attuale in coordinates_dict: # Nessun indirizzo, ma abbiamo coordinate
                        try:
                            if is_within_comune_boundaries(
                                float(elemento_lat),
                                float(elemento_lon),
                                coordinates_dict[comune_attuale]
                            ):
                                salvare_risultato = True
                                motivazione_geo = f"Senza indirizzo, ma entro i confini di '{comune_attuale}' tramite coordinate (Lat: {elemento_lat}, Lon: {elemento_lon})."
                                logger.info(f"Risultato '{name}' (senza indirizzo) confermato nel comune '{comune_attuale}' tramite coordinate.")
                            else:
                                motivazione_geo = f"Senza indirizzo, FUORI dai confini di '{comune_attuale}' tramite coordinate (Lat: {elemento_lat}, Lon: {elemento_lon}). Raggio comune: {coordinates_dict[comune_attuale].get('radius')}km."
                                logger.info(f"Risultato '{name}' (senza indirizzo) escluso: fuori dai confini del comune '{comune_attuale}' in base alle coordinate.")
                        except ValueError:
                             motivazione_geo = f"Errore conversione coordinate per '{name}' (senza indirizzo): lat={elemento_lat}, lon={elemento_lon}. Escluso."
                             logger.warning(motivazione_geo)
                        except KeyError:
                            motivazione_geo = f"Coordinate per '{comune_attuale}' non in coordinates_dict. Escluso '{name}' (senza indirizzo)."
                            logger.warning(motivazione_geo)
                    else:
                        motivazione_geo = f"Indirizzo mancante e controllo coordinate non possibile per '{name}'."
                        logger.info(motivazione_geo)

                    # Log della decisione finale
                    if salvare_risultato:
                        logger.info(f"DECISIONE GEO FINALE: AGGIUNGENDO '{name}' per ricerca in '{comune_attuale}'. Motivo: {motivazione_geo}")
                        results.append(result)
                    else:
                        logger.info(f"DECISIONE GEO FINALE: NON AGGIUNGENDO '{name}' per ricerca in '{comune_attuale}'. Motivo: {motivazione_geo}")
                    
                    # Torna alla pagina dei risultati usando l'URL originale anziché il pulsante Back
                    try:
                        driver.get(current_list_page_url) # Usa l'URL della pagina elenco salvato
                        logger.info(f"Tornati alla pagina risultati via URL: {current_list_page_url}")
                        time.sleep(3)
                    except Exception as nav_error:
                        logger.error(f"Errore nella navigazione all'URL originale della lista: {nav_error}")
                        try:
                            driver.back()
                            time.sleep(3)
                            logger.info("Tornati alla pagina risultati via Back button")
                        except:
                            driver.get(url) # Fallback all'URL di ricerca originale
                            time.sleep(5)
                            logger.info("Ricerca ricaricata completamente come fallback")
                
                except Exception as e:
                    logger.error(f"Errore nell'estrazione del risultato: {str(e)}")
                    # Screenshot per debug
                    screenshot_filename = f"error_{comune_attuale}_{keyword}_{i}.png".replace(" ", "_")
                    driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
                    
                    # Ripristina la pagina dei risultati
                    try:
                        driver.get(url)
                        time.sleep(5)
                        logger.info("Pagina di ricerca ricaricata dopo errore")
                        
                        # Ritrova i risultati
                        for selector in selectors_to_try:
                            try:
                                temp_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                if temp_elements and len(temp_elements) > 0:
                                    result_elements = temp_elements
                                    used_selector = selector
                                    break
                            except:
                                continue
                    except:
                        logger.error("Impossibile ripristinare la pagina di ricerca")
            
            # Screenshot finale per questa ricerca
            screenshot_filename = f"completed_{comune_attuale}_{keyword}.png".replace(" ", "_")
            driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
            
            # Rispetta i rate limit di Google con pausa variabile
            pause_time = random.uniform(8, 12)
            logger.info(f"Pausa di {pause_time:.2f} secondi tra le ricerche")
            time.sleep(pause_time)
            
        except Exception as e:
            logger.error(f"Errore generale per {keyword} {comune_attuale}: {str(e)}")
            screenshot_filename = f"error_general_{comune_attuale}_{keyword}.png".replace(" ", "_")
            driver.save_screenshot(os.path.join(SCREENSHOT_DIR, screenshot_filename))
    
    driver.quit()    
    return results

# Funzione aggiuntiva per verificare se un risultato è all'interno dei confini comunali
def is_within_comune_boundaries(lat, lon, comune_coords, max_distance_km=15):
    """Verifica se una posizione è all'interno dei confini di un comune"""
    if not lat or not lon or not comune_coords:
        return False
    
    try:
        # Calcola la distanza tra due punti geografici
        def haversine(lat1, lon1, lat2, lon2):
            R = 6371  # Raggio della Terra in km
            dLat = math.radians(lat2 - lat1)
            dLon = math.radians(lon2 - lon1)
            a = (math.sin(dLat/2) * math.sin(dLat/2) +
                 math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
                 math.sin(dLon/2) * math.sin(dLon/2))
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            distance = R * c
            return distance
        
        # Calcola la distanza tra il punto e il centro del comune
        distance = haversine(lat, lon, comune_coords['lat'], comune_coords['lon'])
        
        # Determina se è all'interno del raggio del comune
        # Usiamo max_distance_km come limite superiore per evitare falsi positivi
        max_radius = min(comune_coords['radius'], max_distance_km)
        
        return distance <= max_radius
        
    except Exception as e:
        logging.error(f"Errore nel calcolo della distanza: {str(e)}")
        return False

def clean_url(url):
    """Pulisce l'URL del sito web (rimuove percorsi e parametri)"""
    if not url:
        return ""
    
    # Rimuovi mailto: se presente
    url = url.replace("mailto:", "")
    
    # Analizziamo l'URL
    try:
        parsed = urlparse(url)
        
        # Costruisci l'URL base (schema + netloc)
        clean = f"{parsed.scheme}://{parsed.netloc}"
        
        return clean
    except:
        return url

def _same_domain(url: str, candidate: str) -> bool:
    try:
        u = urlparse(url)
        c = urlparse(candidate)
        return (u.netloc.split(':')[0].lower().replace('www.', '') == c.netloc.split(':')[0].lower().replace('www.', ''))
    except:
        return False

def find_contact_subpages(base_url):
    """Prova a scoprire rapidamente le sotto-pagine di contatto partendo dall'homepage e dalla sitemap.

    Ritorna una lista ordinata di URL candidati (priorità più alta per match più forti).
    """
    if not base_url:
        return []
    try:
        base = clean_url(base_url)
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })

        candidates = []

        # 1) Sitemap discovery via robots.txt
        try:
            robots_url = base.rstrip('/') + '/robots.txt'
            r = session.get(robots_url, timeout=6)
            if r.status_code == 200 and 'Sitemap:' in r.text:
                for line in r.text.splitlines():
                    if line.lower().startswith('sitemap:'):
                        sm_url = line.split(':', 1)[1].strip()
                        if sm_url.startswith('http'):
                            try:
                                sm = session.get(sm_url, timeout=8)
                                if sm.status_code == 200 and '<urlset' in sm.text:
                                    # Estrai <loc>
                                    for loc in re.findall(r'<loc>\s*([^<\s]+)\s*</loc>', sm.text, flags=re.IGNORECASE):
                                        if _same_domain(base, loc) and any(k in loc.lower() for k in [
                                            'contact', 'contacts', 'contact-us', 'contactus',
                                            'contatto', 'contatti', 'chi-siamo', 'chisiamo', 'about'
                                        ]):
                                            candidates.append(loc)
                            except:
                                pass
        except:
            pass

        # 2) Parse homepage links
        try:
            hp = session.get(base, timeout=8)
            if hp.status_code == 200:
                soup = BeautifulSoup(hp.text, 'html.parser')
                anchors = soup.find_all('a', href=True)
                for a in anchors:
                    href = a['href'].strip()
                    text = (a.get_text() or '').strip().lower()
                    # Normalizza href relativo → assoluto
                    abs_url = href
                    if href.startswith('/'):
                        abs_url = base.rstrip('/') + href
                    elif not href.startswith('http'):
                        continue
                    if not _same_domain(base, abs_url):
                        continue
                    href_l = abs_url.lower()
                    score = 0
                    # Keyword score
                    if any(k in href_l for k in ['contatto', 'contatti', 'contact', 'contact-us', 'contacts', 'contactus']):
                        score += 3
                    if any(k in text for k in ['contatti', 'contatto', 'contact']):
                        score += 2
                    if any(k in href_l for k in ['chi-siamo', 'chisiamo', 'about']):
                        score += 1
                    if score > 0:
                        candidates.append((score, abs_url))
        except:
            pass

        # 3) Aggiungi slug comuni come fallback
        common = [
            '/contatti', '/contatti/', '/contatto', '/contattaci', '/contact', '/contacts', '/contact-us', '/contactus',
            '/chi-siamo', '/about', '/azienda', '/company'
        ]
        for slug in common:
            candidates.append((1, base.rstrip('/') + slug))

        # Dedup e ordinamento per score (i tuple hanno score, gli URL puri da sitemap diamo score 4 di default)
        scored = []
        seen = set()
        for item in candidates:
            if isinstance(item, tuple):
                score, link = item
            else:
                score, link = 4, item
            if link not in seen:
                seen.add(link)
                scored.append((score, link))

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
    domain = urlparse(url).netloc.replace('www.', '')
    
    # Domini da ignorare (provider generici, servizi temporanei, blacklist)
    IGNORED_EMAIL_DOMAINS = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "live.com", "msn.com", "icloud.com",
        "sentry.io", "wixpress.com", "example.com", "test.com", "yourdomain.com", "mydomain.com", "website.com", "domain.com", "localhost",
        "google.com", "facebook.com", "twitter.com", "instagram.com", "doubleclick.net", "amazonaws.com", "appspot.com", "cdn.com", "cloudfront.net",
        "windows.net", "azure.com", "microsoft.com", "apple.com"
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
        except:
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
                            raise e
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
 
def _extract_emails_with_driver(url, driver):
    """Fallback con Selenium per estrarre email quando le richieste HTTP falliscono o non trovano nulla."""
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
                html = re.sub(r"\\s*\\[at\\]\\s*|\\s*\\(at\\)\\s*|\\sat\\s", "@", html, flags=re.IGNORECASE)
                html = re.sub(r"\\s*\\[dot\\]\\s*|\\s*\\(dot\\)\\s*|\\spunto\\s|\\sdot\\s", ".", html, flags=re.IGNORECASE)
                emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}", html)
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

# Funzione migliorata per la pulizia di testo estratto da elementi HTML
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

def search_contact_info(company_name, log_file="log_google_snippet.csv"): 
    """Cerca informazioni sul contatto usando Google Search, estraendo titoli e snippet/metadescrizioni da più selettori e loggando i risultati."""
    logger.info(f"Inizio ricerca contatti per: {company_name}")
    logger.info(f"File di log impostato su: {os.path.abspath(log_file)}")
    
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
                    delay = random.uniform(2, 4)
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
                        debug_file = f"debug_google_response_{i}.html"
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(response.text)
                        logger.info(f"HTML salvato in {debug_file} per debug")
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
                logger.error(f"Directory corrente: {os.getcwd()}")
                logger.error(f"Permessi directory: {oct(os.stat('.').st_mode)[-3:]}")
        else:
            logger.warning(f"Nessun risultato da loggare per {company_name}")
        
        return search_results
        
    except Exception as e:
        logger.error(f"Errore generale nella ricerca contatti per {company_name}: {str(e)}")
        return []

def extract_contact_person(company_info, api_key):
    """Estrae informazioni di contatto da Google Search usando Selenium"""
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
        driver.implicitly_wait(10)
        
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
        
        # Attesa per il caricamento iniziale
        time.sleep(random.uniform(8, 10))
        
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
                consent_button = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((selector_type, selector))
                )
                driver.execute_script("arguments[0].click();", consent_button)
                time.sleep(random.uniform(3, 4))
                logger.info(f"Cookie accettati per {company_name}")
                break
            except:
                continue
        
        # Attendi il caricamento dei risultati
        time.sleep(random.uniform(6, 8))
        
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
                search_div = WebDriverWait(driver, 15).until(
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
            logger.info(f"Primi 500 caratteri del body: {driver.find_element(By.TAG_NAME, 'body').text[:500]}")
        
    except Exception as e:
        logger.error(f"Errore generale nella ricerca contatti per {company_name}: {str(e)}")
    
    finally:
        if driver:
            try:
                driver.quit()
                logger.info(f"Driver chiuso per {company_name}")
            except:
                pass
    
    return company_info

def save_to_csv(results, output_file):
    """Salva i risultati in un file CSV"""
    if not results:
        logger.warning("Nessun risultato da salvare.")
        return
    
    # Assicurati che tutti i risultati abbiano il campo 'contatto'
    for result in results:
        if 'contatto' not in result:
            result['contatto'] = ''
        elif result['contatto'] is None:
            result['contatto'] = ''
    
    # Assicurati che tutti i risultati abbiano tutti i campi necessari
    required_fields = ['comune', 'keyword', 'nome', 'indirizzo', 'telefono', 'sito_web', 
                      'num_recensioni', 'tipo', 'email', 'linkedin', 'pertinenza', 
                      'categoria', 'confidenza_analisi', 'contatto', 'distanza_km']
    
    for result in results:
        for field in required_fields:
            if field not in result:
                result[field] = ''
    
    fieldnames = required_fields
    
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    logger.info(f"Risultati salvati in {output_file}")

def _ensure_required_fields(result):
    required_fields = ['comune', 'keyword', 'nome', 'indirizzo', 'telefono', 'sito_web', 
                      'num_recensioni', 'tipo', 'email', 'linkedin', 'pertinenza', 
                      'categoria', 'confidenza_analisi', 'contatto', 'distanza_km']
    for field in required_fields:
        if field not in result or result[field] is None:
            result[field] = ''
    return required_fields

def _clean_result_fields(result):
    # Pulizia campi testuali, replica della logica finale per scrittura immediata
    if result.get("indirizzo"):
        result["indirizzo"] = clean_extracted_text(result["indirizzo"])
        result["indirizzo"] = re.sub(r'^,\s*', '', result["indirizzo"]) if result["indirizzo"] else ''
    if result.get("telefono"):
        result["telefono"] = clean_extracted_text(result["telefono"])
        result["telefono"] = re.sub(r'^[,\s]+', '', result["telefono"]) if result["telefono"] else ''
    if result.get("nome"):
        result["nome"] = clean_extracted_text(result["nome"])
    if result.get("email"):
        emails = result["email"].split(", ") if isinstance(result["email"], str) else []
        clean_emails = [clean_extracted_text(email) for email in emails]
        clean_emails = [email for email in clean_emails if email]
        result["email"] = ", ".join(clean_emails)
        result["email"] = re.sub(r'^,\s*', '', result["email"]) if result["email"] else ''

def _make_result_key(result):
    # Chiave di dedup per resume: (nome, comune, sito_web) in lower
    return (str(result.get('nome','')).strip().lower(),
            str(result.get('comune','')).strip().lower(),
            clean_url(str(result.get('sito_web','')).strip().lower()) if result.get('sito_web') else '')

def _ensure_csv_header(output_file, fieldnames):
    file_exists = os.path.exists(output_file)
    if not file_exists:
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

def _append_result_to_csv(result, output_file, fieldnames):
    _ensure_csv_header(output_file, fieldnames)
    with open(output_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerow(result)

def _load_existing_keys(output_file):
    keys = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    key = (str(row.get('nome','')).strip().lower(),
                           str(row.get('comune','')).strip().lower(),
                           clean_url(str(row.get('sito_web','')).strip().lower()) if row.get('sito_web') else '')
                    keys.add(key)
        except Exception as e:
            logger.error(f"Impossibile caricare chiavi esistenti da {output_file}: {e}")
    return keys

def enrich_and_filter_results(results, analyzer, driver, output_file=None, existing_keys=None):
    """Arricchisce i risultati con informazioni aggiuntive e filtra per pertinenza e dimensione"""
    enriched_filtered_results = []
    
    # Lista di parole chiave che potrebbero indicare grandi imprese
    big_company_keywords = [
        "enel", "eni", "edison", "a2a", "sorgenia", "iren", "hera", "vivi energia", 
        "engie", "acea", "e.on", "axpo", "multinazionale", "gruppo", "corporation", 
        "holding", "s.p.a.", "spa"
    ]
    
    total = len(results)
    # Prepara header CSV se in modalità append streaming
    required_fields_for_csv = ['comune', 'keyword', 'nome', 'indirizzo', 'telefono', 'sito_web', 
                      'num_recensioni', 'tipo', 'email', 'linkedin', 'pertinenza', 
                      'categoria', 'confidenza_analisi', 'contatto', 'distanza_km']
    if output_file:
        _ensure_csv_header(output_file, required_fields_for_csv)
    if existing_keys is None:
        existing_keys = set()

    for i, result in enumerate(results):
        logger.info(f"Elaborazione risultato {i+1} di {total}: {result['nome']}")
        
        # Controlla se è una grande impresa
        nome_lower = result["nome"].lower()
        is_big_company = any(kw in nome_lower for kw in big_company_keywords)
        
        # Se è una grande impresa, saltiamo
        if is_big_company:
            logger.info(f"Saltata grande impresa: {result['nome']}")
            continue
        
        # Pulisci l'URL del sito web
        result["sito_web"] = clean_url(result["sito_web"]) if result.get("sito_web") else ''

        # Skip se già presente in output (resume)
        key = _make_result_key(result)
        if key in existing_keys:
            logger.info(f"Risultato già presente in output, salto: {result['nome']} - {result.get('sito_web','')}")
            continue
        
        # Cerca il nome dell'amministratore (fallback HTTP se Selenium non disponibile)
        if driver is None:
            try:
                snippet_results = search_contact_info(result["nome"]) or []
                texts = []
                for item in snippet_results:
                    title = item.get("title")
                    snippet = item.get("snippet")
                    if title:
                        texts.append(title)
                    if snippet:
                        texts.append(snippet)
                admin_name = extract_admin_with_gpt("\n".join(texts)) if texts else None
            except Exception as e:
                logger.error(f"Errore nel fallback HTTP per admin di {result['nome']}: {e}")
                admin_name = None
        else:
            admin_name = test_single_query(driver, result["nome"], f"{result['nome']} amministratore")
        if admin_name:
            result["contatto"] = admin_name
            logger.info(f"Nome amministratore trovato per {result['nome']}: {admin_name}")
        else:
            result["contatto"] = ""
            logger.info(f"Nessun amministratore trovato per {result['nome']}")
        
        # Analizza la pertinenza del sito web
        if result["sito_web"]:
            relevance = analyzer.analyze_website_relevance(result["sito_web"])
            result["pertinenza"] = relevance["is_relevant"]
            result["categoria"] = relevance["category"]
            result["confidenza_analisi"] = relevance["confidence"]
            
            # Estrai email e LinkedIn anche se non marcato come pertinente (ma con priorità se lo è)
            logger.info(f"Estrazione email/LinkedIn da: {result['sito_web']}")
            contact_data = extract_emails_from_website(result["sito_web"], disable_slug_fallback=False) or []
            if not contact_data and driver is not None:
                logger.info("Nessuna email trovata via HTTP. Provo fallback Selenium...")
                try:
                    selenium_emails = _extract_emails_with_driver(result["sito_web"], driver)
                    contact_data.extend(selenium_emails)
                except Exception as e:
                    logger.debug(f"Fallback Selenium per email fallito: {e}")
            
            emails = []
            linkedin_links = []
            for item in contact_data:
                if isinstance(item, str) and item.startswith("LINKEDIN:"):
                    linkedin_links.append(item.replace("LINKEDIN:", ""))
                elif isinstance(item, str):
                    emails.append(item)
            
            unique_emails = []
            for email in emails:
                if email not in unique_emails:
                    unique_emails.append(email)
            
            result["email"] = ", ".join(unique_emails)
            result["linkedin"] = ", ".join(linkedin_links)
            
            # Completa campi mancanti, pulisci e scrivi subito
            _ensure_required_fields(result)
            _clean_result_fields(result)
            enriched_filtered_results.append(result)
            if output_file:
                _append_result_to_csv(result, output_file, required_fields_for_csv)
                existing_keys.add(_make_result_key(result))
        else:
            # Se non c'è un sito web, verifichiamo il nome e le parole chiave di ricerca
            keyword_lower = result["keyword"].lower()
            nome_lower = result["nome"].lower()
            
            if any(kw in nome_lower for kw in analyzer.fotovoltaico_keywords) or \
               any(kw in nome_lower for kw in analyzer.domotica_keywords) or \
               "solar" in nome_lower or "energi" in nome_lower:
                
                result["pertinenza"] = True
                result["categoria"] = "fotovoltaico" if any(kw in nome_lower for kw in analyzer.fotovoltaico_keywords) else "domotica"
                result["confidenza_analisi"] = 0.6  # Confidenza moderata basata solo sul nome
                
                _ensure_required_fields(result)
                _clean_result_fields(result)
                enriched_filtered_results.append(result)
                if output_file:
                    _append_result_to_csv(result, output_file, required_fields_for_csv)
                    existing_keys.add(_make_result_key(result))
    
    return enriched_filtered_results

def deduplicate_results(results):
    """Deduplicazione dei risultati basata su nome e indirizzo"""
    unique_results = {}
    
    for result in results:
        # Creiamo una chiave unica combinando nome e indirizzo
        key = f"{result['nome']}|{result['indirizzo']}"
        
        if key not in unique_results:
            unique_results[key] = result
        else:
            # Se abbiamo già questo risultato, aggiorniamo alcuni campi se mancanti
            existing = unique_results[key]
            
            if not existing["telefono"] and result["telefono"]:
                existing["telefono"] = result["telefono"]
            
            if not existing["sito_web"] and result["sito_web"]:
                existing["sito_web"] = result["sito_web"]
            
            if not existing["email"] and result["email"]:
                existing["email"] = result["email"]
            
            if not existing["linkedin"] and result["linkedin"]:
                existing["linkedin"] = result["linkedin"]
            
            # Se la confidenza di pertinenza è maggiore, aggiorniamo categoria e confidenza
            if result["confidenza_analisi"] > existing["confidenza_analisi"]:
                existing["categoria"] = result["categoria"]
                existing["confidenza_analisi"] = result["confidenza_analisi"]
    
    return list(unique_results.values())

def extract_admin_with_gpt(texts):
    """Estrae il nome dell'amministratore dai testi usando GPT"""
    try:
        # Prepara il prompt per GPT
        prompt = f"""Analizza il seguente testo e estrai il nome dell'amministratore o del legale rappresentante dell'azienda.
        Se non trovi un nome specifico, rispondi con 'Nessun amministratore trovato'.
        Rispondi SOLO con il nome dell'amministratore, senza altre parole.

        Testo da analizzare:
        {texts}"""

        # Chiamata alla nuova API di OpenAI
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Sei un assistente specializzato nell'estrazione di nomi di amministratori da testi."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=50
        )

        # Estrai la risposta
        admin_name = response.choices[0].message.content.strip()
        
        # Verifica se la risposta indica che non è stato trovato nessun nome
        if any(phrase in admin_name.lower() for phrase in ["nessun amministratore", "non ho trovato", "non è possibile"]):
            return None
            
        return admin_name

    except Exception as e:
        logger.error(f"Errore nell'estrazione del nome dell'amministratore con GPT: {str(e)}")
        return None

def test_single_query(driver, company_name, query):
    """Test di una singola query di ricerca"""
    logger.info(f"\n{'='*50}\nTest query per: {company_name}\n{'='*50}")
    
    # Se non abbiamo un driver Selenium (es. flusso Places API), salta la ricerca admin
    if driver is None:
        logger.info("Driver Selenium non disponibile: salto ricerca amministratore per questa esecuzione.")
        return None

    try:
        # Costruisci l'URL di ricerca
        encoded_query = query.replace(' ', '+')
        url = f"https://www.google.com/search?q={encoded_query}&hl=it"
        
        # Carica la pagina
        driver.get(url)
        logger.info(f"Pagina caricata per {company_name}")
        
        # Attesa per il caricamento iniziale
        time.sleep(random.uniform(8, 10))
        
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
                consent_button = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((selector_type, selector))
                )
                driver.execute_script("arguments[0].click();", consent_button)
                time.sleep(random.uniform(3, 4))
                logger.info(f"Cookie accettati per {company_name}")
                break
            except:
                continue
        
        # Attendi il caricamento dei risultati
        time.sleep(random.uniform(6, 8))
        
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
                search_div = WebDriverWait(driver, 15).until(
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
            
            # Raccogli tutti i testi per l'analisi GPT
            all_texts = []
            
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
                        
                        # Aggiungi il testo alla lista per l'analisi GPT
                        if title:
                            all_texts.append(title)
                        if snippet:
                            all_texts.append(snippet)
                        
                except Exception as e:
                    logger.error(f"Errore nell'estrazione del risultato {i} per {company_name}: {str(e)}")
            
            # Usa GPT per estrarre il nome dell'amministratore
            if all_texts:
                admin_name = extract_admin_with_gpt("\n".join(all_texts))
                if admin_name:
                    logger.info(f"Nome amministratore trovato per {company_name}: {admin_name}")
                    return admin_name
                else:
                    logger.info(f"Nessun amministratore trovato per {company_name}")
            else:
                logger.info(f"Nessun testo trovato per l'analisi GPT per {company_name}")
                
        else:
            logger.error(f"Nessun elemento di ricerca trovato per {company_name}!")
            logger.info(f"Contenuto della pagina per {company_name}:")
            logger.info(f"Titolo: {driver.title}")
            logger.info(f"URL attuale: {driver.current_url}")
            logger.info(f"Primi 500 caratteri del body: {driver.find_element(By.TAG_NAME, 'body').text[:500]}")
        
    except Exception as e:
        logger.error(f"Errore generale nella ricerca per {company_name}: {str(e)}")
    
    return None

def main():
    # Inizializza l'analizzatore di pertinenza
    analyzer = WebsiteRelevanceAnalyzer()
    
    # Creazione della cartella degli screenshot se non esiste
    if not os.path.exists(SCREENSHOT_DIR):
        os.makedirs(SCREENSHOT_DIR)
        logger.info(f"Cartella screenshot creata: {SCREENSHOT_DIR}")
    
    # 1. Carica la lista dei comuni
    print("Caricamento della lista dei comuni...")
    comuni_file = input("Inserisci nome file della lista comuni desiderata:") # File predefinito
    all_comuni = load_comuni(comuni_file)
    print(f"Caricati {len(all_comuni)} comuni dal file.")
    
    # 1.1. Filtra comuni già elaborati e limita a 5 per esecuzione
    comuni = filter_and_limit_comuni(all_comuni, max_comuni=10)
    
    if not comuni:
        print("Tutti i comuni sono già stati elaborati!")
        return
    
    print(f"Comuni selezionati per questa esecuzione: {comuni}")
    
    # Aggiunto: Costruisci il dizionario delle coordinate dei comuni
    print("Recupero coordinate per i comuni...")
    comunes_coordinates_dict = build_comune_coordinates_dict(comuni)
    if not comunes_coordinates_dict:
        logger.error("Nessuna coordinata recuperata per i comuni. Il programma potrebbe non funzionare come previsto.")
        # Potresti voler terminare o gestire diversamente questo caso
    
    # 2. Genera URL di ricerca utilizzando le coordinate
    print("Generazione URL di ricerca con coordinate...")
    # Utilizza la funzione che genera URL con dettagli di geolocalizzazione
    search_urls = generate_search_urls_with_coordinates(comuni, KEYWORDS, comunes_coordinates_dict)
    print(f"Generati {len(search_urls)} URL di ricerca.")
    
    # 3. Seleziona metodo di raccolta risultati: 1=SerpAPI, 2=Selenium, 3=Google Places API (consigliato)
    scraping_method = "3"
    
    results = [] # Inizializza results
    driver = None
    
    if scraping_method == "1":
        # Utilizzo di SerpAPI
        if API_KEY == "TUA_API_KEY_SERPAPI" or not API_KEY:
            API_KEY = input("Inserisci la tua API key di SerpAPI: ")
        
        print("Avvio scraping con SerpAPI (modalità geolocalizzata)...")
        # Utilizza scrape_with_serpapi_geo che sfrutta le coordinate negli search_urls
        results = scrape_with_serpapi_geo(search_urls, API_KEY)
    elif scraping_method == "2":
        # Utilizzo di Selenium
        print("Avvio scraping con Selenium...")
        # Configura il driver
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        driver = webdriver.Chrome(service=webdriver.ChromeService(ChromeDriverManager().install()), options=chrome_options)
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        results = scrape_with_selenium(search_urls, comunes_coordinates_dict)
    elif scraping_method == "3":
        # Utilizzo di Google Places API
        api_key = GOOGLE_PLACES_API_KEY
        if not api_key:
            api_key = input("Inserisci la tua API key di Google Places/Maps: ")
        print("Avvio raccolta con Google Places API...")
        logging.info("Avvio con [PlacesAPI]...")
        # Ricicliamo search_urls che contengono comune, keyword, lat, lon, radius
        # Determina la modalità runtime (preferisci env runtime rispetto al valore iniziale)
        use_details = resolve_use_details()
        logging.info(f"[PlacesAPI] Modalità sito: {'Details' if use_details else 'WebSearch'} (mode_str={mode_str})")
        results, counters = scrape_with_places_api(search_urls, api_key, fetch_details=use_details, per_query_limit=None)
        _append_api_usage_log(counters)
        # Inizializza Selenium per la fase di enrichment anche nel ramo Places API
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", False)
            driver = webdriver.Chrome(service=webdriver.ChromeService(ChromeDriverManager().install()), options=chrome_options)
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        except Exception as e:
            logger.error(f"Impossibile inizializzare Selenium per enrichment nel ramo Places API: {e}")
    else:
        print("Scelta non valida. Uscita.")
        return
    
    print(f"Raccolti {len(results)} risultati grezzi.")
    
    # 4. Deduplicazione dei risultati
    print("Deduplicazione dei risultati iniziali...")
    unique_results = deduplicate_results(results)
    print(f"Risultati unici: {len(unique_results)}")
    
    # 5. Analisi di pertinenza e arricchimento dei risultati con salvataggio incrementale
    print("Analisi di pertinenza e arricchimento dei risultati (salvataggio incrementale)...")
    existing_keys = _load_existing_keys(OUTPUT_FILE)
    filtered_results = enrich_and_filter_results(unique_results, analyzer, driver, output_file=OUTPUT_FILE, existing_keys=existing_keys)
    print(f"Risultati pertinenti trovati e scritti: {len(filtered_results)}")
    
    # 9. Salva i comuni elaborati nel log
    _save_processed_comuni(comuni)
    
    # 10. Chiudi il driver se è stato creato
    if driver:
        driver.quit()
    
    print(f"Processo completato! Salvati {len(unique_results)} risultati in {OUTPUT_FILE}")
    print(f"Comuni elaborati salvati nel log: {comuni}")
if __name__ == "__main__":
    try:
        import argparse
        parser = argparse.ArgumentParser(description="Utility di scraping e enrichment")
        parser.add_argument("--test-email", dest="test_email", help="URL del sito su cui testare l'estrazione email")
        parser.add_argument("--use-selenium", dest="use_selenium", action="store_true", help="Usa Selenium come fallback per l'estrazione email")
        parser.add_argument("--places-details-mode", dest="places_details_mode", choices=["web", "details"], help="Modalità per ottenere sito in Places: web (default) o details")
        args, unknown = parser.parse_known_args()
        
        if args.test_email:
            url = args.test_email
            print(f"Test estrazione email via HTTP da: {url}")
            emails = extract_emails_from_website(url, disable_slug_fallback=True)
            print(f"Email trovate (HTTP): {emails}")
            
            if args.use_selenium and not emails:
                print("Nessuna email via HTTP. Inizializzo Selenium fallback...")
                try:
                    chrome_options = Options()
                    chrome_options.add_argument("--headless")
                    chrome_options.add_argument("--no-sandbox")
                    chrome_options.add_argument("--disable-dev-shm-usage")
                    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                    chrome_options.add_experimental_option("useAutomationExtension", False)
                    driver = webdriver.Chrome(service=webdriver.ChromeService(ChromeDriverManager().install()), options=chrome_options)
                    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
                    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    emails = _extract_emails_with_driver(url, driver)
                except Exception as e:
                    print(f"Errore inizializzazione Selenium: {e}")
                    emails = emails or []
                finally:
                    try:
                        driver.quit()
                    except:
                        pass
                print(f"Email trovate (Selenium): {emails}")
            
            # Salva debug
            try:
                debug_file = "debug_emails.csv"
                if not os.path.exists(debug_file):
                    with open(debug_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['Dominio', 'Email Trovate', 'URL'])
                domain = urlparse(url).netloc.replace('www.', '')
                with open(debug_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([domain, '; '.join(emails or []), url])
                print(f"Debug scritto in {debug_file}")
            except Exception as e:
                print(f"Errore scrittura debug: {e}")
            
        else:
            if args.places_details_mode:
                os.environ["PLACES_DETAILS_MODE"] = args.places_details_mode
            main()
    except SystemExit:
        # argparse ha già stampato l'help se necessario
        pass