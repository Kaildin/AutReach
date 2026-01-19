# AutReach SaaS

AutReach is a simple, modern lead generation and enrichment pipeline designed for efficient business discovery. It combines Google Maps/Places scraping, intelligent website relevance analysis, and automated contact extraction with administrative discovery.

## 🚀 Main Features

- **Multi-Source Scraping**: Support for Google Places API and Selenium-based scraping to gather leads from Google Maps.
- **Intelligent Relevance Analysis**: Automatic evaluation of website content to ensure business relevance based on specific industry keywords.
- **Contact Extraction**: Deep crawling for emails and LinkedIn profiles with sitemap support and Selenium fallbacks.
- **Admin Discovery (Advanced)**: Dedicated pipeline to find company administrators using DuckDuckGo search and GPT-4 extraction.
- **Streamlit Dashboard**: A user-friendly web interface to manage scraping and enrichment tasks.
- **Resilience**: Integrated rate-limiting handling, automatic backups, and IP rotation via Tor for legacy verification steps.

## 📂 Project Structure

```text
outreach_saas/
├── app.py                      # Streamlit Dashboard
├── src/
│   └── outreach_saas/
│       ├── main.py              # Main Pipeline Entry Point
│       ├── pipelines/           # Core Logic (main_pipeline, admin_pipeline)
│       ├── scraping/            # Scrapers (Selenium, Places API, DDG, Search Utils)
│       ├── analysis/            # Relevance Analyzer (GPT-powered)
│       ├── config/              # Centralized Settings & Definitions
│       └── utils/               # Shared File, Geo, and Text Utilities
└── output/                      # Default directory for generated CSVs
```

## 🛠️ Setup

### Prerequisites

- Python 3.10+
- Chrome/Chromium (for Selenium)
- Tor (optional, for legacy email verification)

### Installation

1. Clone the repository and navigate to the directory:
   ```bash
   git clone [repository-url]
   cd outreach_saas
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables in a `.env` file:
   ```ini
   GOOGLE_PLACES_API_KEY=your_google_key
   OPENAI_API_KEY=your_openai_key
   SCRAPING_METHOD=places  # or 'selenium'
   ```

## 📖 Usage

### Streamlit Dashboard (Recommended)
Launch the interactive web interface:
```bash
streamlit run app.py
```

### Command Line Interface

**1. Main Scraping Pipeline:**
```bash
PYTHONPATH=src python3 -m outreach_saas.main --industry fotovoltaico --input comuni.csv
```

**2. Admin Discovery Pipeline:**
```bash
PYTHONPATH=src python3 -m outreach_saas.pipelines.admin_pipeline --input output/aziende_filtrate.csv --output output/with_admins.csv
```

## 📊 CSV Structure

The final output includes fields such as:
- `nome`, `comune`, `indirizzo`, `telefono`, `sito_web`
- `email`, `linkedin`
- `contatto` (Administrative name found via search)
- `pertinenza`, `categoria`, `confidenza_analisi`

## 🛡️ License
[MIT License](LICENSE)
