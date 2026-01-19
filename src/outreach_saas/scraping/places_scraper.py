import logging
import requests
import json
import os
import time
import random
from typing import List, Dict, Any, Tuple
from .search_utils import find_official_site_via_web

logger = logging.getLogger(__name__)

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

def scrape_with_places_api(search_urls, api_key, fetch_details=True, per_query_limit=None):
    """Recupera risultati aziende usando Google Places API (Nearby Search + Details)."""
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
    
    def _load_details_cache():
        try:
            if os.path.exists(details_cache_path):
                with open(details_cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.debug(f"[PlacesAPI] Impossibile caricare cache Details: {e}")
        return {}
        
    def _save_details_cache(cache_obj):
        try:
            os.makedirs(os.path.dirname(details_cache_path), exist_ok=True)
            with open(details_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.debug(f"[PlacesAPI] Impossibile salvare cache Details: {e}")

    details_cache = _load_details_cache()
    cache_dirty = False

    for search in search_urls:
        comune = search.get('comune', '')
        keyword = search.get('keyword', '')
        lat = search.get('lat')
        lon = search.get('lon')
        radius_km = search.get('radius', 5)
        radius_m = int(max(1, float(radius_km)) * 1000)

        logger.info(f"[PlacesAPI] Cercando: '{keyword}' in {comune} (lat: {lat}, lon: {lon}, raggio: {radius_m} m)")

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
                logger.error(f"[PlacesAPI] Errore richiesta Nearby Search per {keyword} {comune}: {e}")
                break

            status = data.get('status')
            if status not in {"OK", "ZERO_RESULTS"}:
                logger.warning(f"[PlacesAPI] Stato non OK: {status} - messaggio: {data.get('error_message', '')}")
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
                distance_km = "" # Calcolo reale richiederebbe haversine qui

                website = ""
                phone = ""

                # Evita duplicati per stesso place_id nella stessa esecuzione
                if place_id and place_id in seen_place_ids:
                    continue
                if place_id:
                    seen_place_ids.add(place_id)

                # Decide se fare Details
                if fetch_details and place_id:
                    cached = details_cache.get(place_id, {})
                    website = cached.get('website', '') or website
                    if not website:
                        details_params = {
                            'key': api_key,
                            'place_id': place_id,
                            'fields': 'website'
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
                                    logger.info(f"[PlacesAPI] Sito via Details per {name} ({comune}): {website}")
                            else:
                                logger.debug(f"[PlacesAPI] Details non OK per {name}: {det_data.get('status')}")
                        except Exception as e:
                            logger.debug(f"[PlacesAPI] Errore Details per {name}: {e}")
                elif not fetch_details:
                    # Fallback web search leggero
                    try:
                        if idx < 10 and name:
                            logger.info(f"[PlacesAPI] Provo WebSearch per sito: {name} ({comune}) [rank {idx+1}]")
                            site_guess = find_official_site_via_web(name, comune)
                            if site_guess:
                                website = site_guess
                                logger.info(f"[PlacesAPI] Sito stimato via WebSearch per {name} ({comune}): {website}")
                    except: pass
                
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

                aggregated_results.append(mapped)
                fetched_for_query += 1

            if per_query_limit is not None and fetched_for_query >= per_query_limit:
                break

            page_token = data.get('next_page_token')
            if not page_token:
                break

            time.sleep(1.0)

        time.sleep(random.uniform(0.5, 1.2))

    if cache_dirty:
        _save_details_cache(details_cache)

    return aggregated_results, counters
