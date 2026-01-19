# AutReach
Simple scraping tool to find business info and email verification using google maps data
# Pipeline di Scraping e Verifica Email per Aziende Fotovoltaiche

Questa pipeline è composta da due script principali che lavorano in sequenza per:
1. Raccogliere informazioni sulle aziende del settore fotovoltaico/domotico (`google_scraperP1.py`)
2. Verificare la validità delle email raccolte (`em_verification.py`)

## Panoramica del Sistema

### Fase 1: Scraping e Raccolta Dati (`google_scraperP1.py`)
- Scraping di Google Maps per aziende del settore
- Analisi di pertinenza dei siti web
- Estrazione di contatti (email, LinkedIn)
- Filtraggio e deduplicazione dei risultati

### Fase 2: Verifica Email (`em_verification.py`)
- Verifica delle email raccolte tramite Hunter.io
- Rotazione IP tramite rete Tor
- Gestione automatica di account temporanei
- Backup automatico dei risultati

## Prerequisiti

### Software Richiesto
- Python 3.7 o superiore
- Tor (per la rotazione degli IP)
- Chrome/Chromium (per lo scraping con Selenium)

### Installazione di Tor

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install tor
```

#### macOS (con Homebrew)
```bash
brew install tor
```

#### Windows
Scarica Tor Browser da [torproject.org](https://www.torproject.org/)

## Installazione

1. Clona il repository:
```bash
git clone [url-repository]
cd [nome-directory]
```

2. Installa le dipendenze:
```bash
pip install -r requirements.txt
```

3. Crea il file `requirements.txt`:
```
requests>=2.25.1
pandas>=1.2.0
beautifulsoup4>=4.9.3
stem>=1.8.0
selenium>=4.0.0
webdriver_manager>=3.8.0
```

## Configurazione

### Scraper Google Maps (`google_scraperP1.py`)
```python
API_KEY = "TUA_API_KEY_SERPAPI"  # Se si usa SerpAPI
OUTPUT_FILE = "aziende_fotovoltaico_filtrate.csv"
```

### Verifica Email (`em_verification.py`)
```python
CONFIG = {
    "NUM_ACCOUNTS": 5,
    "TOR_PASSWORD": "",
    "TOR_CONTROL_PORT": 9051,
    "TOR_SOCKS_PORT": 9050,
    "CSV_INPUT_FILE": "aziende_fotovoltaico_filtrate.csv",
    "CSV_OUTPUT_FILE": "aziende_fotovoltaico_verificate.csv"
}
```

## Utilizzo

### 1. Scraping Iniziale
```bash
python google_scraperP1.py
```
Opzioni disponibili:
- Scelta tra SerpAPI o Selenium per lo scraping
- Inserimento file comuni da analizzare
- Configurazione keywords di ricerca

### 2. Verifica Email
```bash
python em_verification.py --input input.csv --output output.csv --accounts 5 --debug
```

Parametri disponibili:
- `--input`: File CSV di input (default: aziende_fotovoltaico_filtrate.csv)
- `--output`: File CSV di output (default: aziende_fotovoltaico_verificate.csv)
- `--accounts`: Numero di account Hunter da creare (default: 5)
- `--backup`: File di backup (default: email_verifications_backup.json)
- `--tor-control-port`: Porta controllo Tor (default: 9051)
- `--tor-socks-port`: Porta SOCKS Tor (default: 9050)
- `--debug`: Attiva logging dettagliato

## Struttura dei File CSV

### Output Fase 1 (`aziende_fotovoltaico_filtrate.csv`)
- nome
- indirizzo
- telefono
- sito_web
- email
- linkedin
- pertinenza
- categoria
- confidenza_analisi

### Output Fase 2 (`aziende_fotovoltaico_verificate.csv`)
Include le colonne precedenti più:
- email_verificata
- email_score
- email_status

## Backup e Ripristino

Il sistema include:
- Backup automatico ogni 10 email verificate
- Salvataggio CSV ogni 20 email
- Backup di emergenza in caso di interruzione
- Possibilità di riprendere da backup esistente

## Gestione degli Errori

- Retry automatico delle richieste fallite
- Rotazione IP tramite Tor
- Gestione interruzioni manuali
- Backup di emergenza

## Limitazioni

- Massimo 50 verifiche per account Hunter.io
- Dipendenza da servizi esterni (mail.tm, Hunter.io)
- Rate limiting di Google Maps
- Necessità di connessione internet stabile

## Note di Sicurezza

- Configurare correttamente Tor
- Non condividere API key
- Monitorare l'utilizzo degli account
- Rispettare i termini di servizio delle piattaforme

## Supporto

Per problemi o domande, aprire una issue nel repository.

## Licenza

[Inserire tipo di licenza] 
