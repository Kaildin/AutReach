import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
try:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
except KeyError:
    # We might want to handle this more gracefully or let it fail if critical
    # raising RuntimeError as in original script
    # raise RuntimeError("Manca la variabile d'ambiente API_KEY nel .env")
    OPENAI_API_KEY = None # Let modules handle the missing key if needed

SCRAPING_METHOD = "selenium"
API_KEY = "TUA_API_KEY_SERPAPI"  # Inserisci la tua API key di SerpAPI
GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY", os.environ.get("GOOGLE_MAPS_API_KEY", ""))

# Other constants
OUTPUT_FILE = "aziende_fotovoltaico_filtrate.csv"
PLACES_DETAILS_MODE = os.environ.get("PLACES_DETAILS_MODE", "web")  # "web" (default) oppure "details"
