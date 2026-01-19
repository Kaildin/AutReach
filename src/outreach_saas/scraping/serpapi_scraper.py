import logging
import requests
import time
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def scrape_with_serpapi_geo(search_urls: List[Dict[str, Any]], api_key: str) -> List[Dict[str, Any]]:
    """Scrape dei risultati utilizzando SerpAPI con parametri di geolocalizzazione precisi"""
    results = []
    
    for search in search_urls:
        comune = search['comune']
        keyword = search['keyword']
        lat = search['lat']
        lon = search['lon']
        radius = search['radius']
        
        logger.info(f"Cercando: {keyword} in {comune} (lat: {lat}, lon: {lon}, raggio: {radius}km)")
        
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
                            "pertinenza": False,
                            "categoria": "",
                            "confidenza_analisi": 0.0,
                            "distanza_km": place.get("distance", "")
                        }
                        results.append(result)
                    else:
                        logger.info(f"Risultato escluso perché fuori dal comune: {place.get('title')}, {address}")
            
            # Rispettiamo i rate limit di SerpAPI
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Errore durante lo scraping di {keyword} {comune}: {str(e)}")
    
    return results
