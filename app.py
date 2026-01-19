
import os
import io
import time
import json
import tempfile
from pathlib import Path
import pandas as pd
import streamlit as st

from helpers import stream_subprocess
import sys
PY_EXE = sys.executable  # il Python con cui sta girando Streamlit (il tuo venv)


APP_TITLE = "Scraper Dashboard"
# Percorsi reali ai tuoi script dentro la repo (relativi alla cartella del presente file)
BASE_DIR = Path(__file__).resolve().parent
SCRAPER_SCRIPT = str(BASE_DIR / "src" / "outreach_saas" / "main.py")
EMAIL_SCRIPT   = str(BASE_DIR / "src" / "outreach_saas" / "legacy" / "em_verification.py")


st.set_page_config(page_title=APP_TITLE, page_icon="🧹", layout="wide")
st.title(APP_TITLE)
st.caption("Interfaccia semplice per eseguire: Google Maps Scraper + Verifica Email (Hunter.io)")

# Sidebar: API Keys & Paths
st.sidebar.header("⚙️ Impostazioni")
st.sidebar.caption(f"Python: `{sys.executable}`")

google_api = st.sidebar.text_input("Google Places API Key", type="password", help="Necessaria per il metodo consigliato nel tuo scraper (Places API).")
openai_api = st.sidebar.text_input("OpenAI API Key", type="password", help="Facoltativa (serve se vuoi l'estrazione nominativi amministratori nel tuo script).")

output_dir = Path(st.sidebar.text_input("Cartella di output (facoltativa)", value=str(Path.cwd() / "output")))
output_dir.mkdir(parents=True, exist_ok=True)
st.sidebar.write(f"Output: `{output_dir}`")

st.sidebar.info("Consiglio: salva le tue API key nella sidebar. Verranno passate al processo in modo sicuro (variabili d'ambiente).")

tabs = st.tabs(["🗺️ Scraping aziende", "📧 Verifica email", "ℹ️ Guida rapida"])

# -------------------- TAB 1: Scraper --------------------
with tabs[0]:
    st.subheader("🗺️ Scraping aziende da Google (Maps/Places)")

    st.markdown("Inserisci i **comuni** (uno per riga) **oppure** carica un file CSV/Excel con una colonna `comune`.")

    comuni_text = st.text_area("Comuni (uno per riga)", height=150, placeholder="Perugia\nTerni\nFoligno")
    comuni_file = st.file_uploader("Oppure carica un file (CSV o XLSX) con la colonna 'comune'", type=["csv", "xlsx"])

    colA, colB = st.columns([1,1])
    with colA:
        per_query_limit = st.number_input("Limite risultati per coppia (comune, keyword) [Places API]", min_value=0, value=0, step=1,
                                          help="0 = nessun limite. Usato solo dal tuo script se lo supporta.")
    with colB:
        run_button = st.button("🚀 Avvia scraping", type="primary")

    log_box = st.empty()
    result_download = st.empty()

    if run_button:
        # Prepare comuni file
        tmp_dir = Path(tempfile.mkdtemp(prefix="scraper_dash_"))
        comuni_path = tmp_dir / "comuni.csv"

        if comuni_file is not None:
            # Save uploaded file
            if comuni_file.name.lower().endswith(".csv"):
                df = pd.read_csv(comuni_file)
            else:
                df = pd.read_excel(comuni_file)
        else:
            # Build DataFrame from textarea
            comuni = [c.strip() for c in comuni_text.splitlines() if c.strip()]
            df = pd.DataFrame({"comune": comuni})

        if "comune" not in df.columns or df.empty:
            st.error("Nessun comune valido trovato. Assicurati di avere una colonna 'comune' o inserire testo.")
        else:
            df.to_csv(comuni_path, index=False)

            # Compose environment for subprocess
            env = os.environ.copy()
            if google_api:
                env["GOOGLE_PLACES_API_KEY"] = google_api
            if openai_api:
                env["OPENAI_API_KEY"] = openai_api

            # Prepare output file path for your script (it uses a constant; we keep it as default)
            # We'll just run the existing script; it asks for file path via input().
            script = SCRAPER_SCRIPT
            # input sequence: it will prompt for "Inserisci nome file della lista comuni desiderata:"
            stdin_payload = f"{comuni_path.name}\n"
            # Run with cwd=tmp_dir so the relative comuni file path works
            cmd = [PY_EXE, script]
            st.info("Esecuzione avviata. I log compaiono sotto. Attendi la fine per i download.")

            logs = []
            for line in stream_subprocess(cmd, cwd=str(tmp_dir), env=env, input_data=stdin_payload):
                logs.append(line.rstrip("\n"))
                # Throttle UI updates a bit
                if len(logs) % 5 == 0:
                    log_box.code("\n".join(logs[-800:]))

            log_box.code("\n".join(logs[-1000:]))

            # Try to find output CSV by name from your script (default 'aziende_fotovoltaico_filtrate.csv')
            possible_files = [
                tmp_dir / "aziende_fotovoltaico_filtrate.csv",
                Path.cwd() / "aziende_fotovoltaico_filtrate.csv"
            ]
            found = None
            for f in possible_files:
                if f.exists() and f.stat().st_size > 0:
                    found = f
                    break

            if found:
                # Copy to chosen output dir with timestamp
                ts_name = f"scraping_{int(time.time())}.csv"
                final_path = output_dir / ts_name
                try:
                    final_path.write_bytes(found.read_bytes())
                except Exception:
                    # Fallback: move
                    os.replace(str(found), str(final_path))

                st.success("Scraping completato! Scarica i risultati:")
                result_download.download_button("⬇️ Scarica CSV", data=final_path.read_bytes(), file_name=ts_name, mime="text/csv")
                st.caption(f"Salvato anche in: `{final_path}`")
            else:
                st.warning("Non ho trovato un CSV di output. Controlla i log sopra.")

# -------------------- TAB 2: Verifica Email --------------------
with tabs[1]:
    st.subheader("📧 Verifica email con Hunter.io")

    st.markdown("Carica il **CSV** con una colonna `email` (o anche altre colonne) e carica un **file .txt** con le API Key di Hunter (una per riga).")
    col1, col2 = st.columns(2)
    with col1:
        input_csv = st.file_uploader("CSV di input (con colonna 'email')", type=["csv"])
    with col2:
        hunter_keys = st.file_uploader("File .txt con API key Hunter.io (una per riga)", type=["txt"])

    out_name = st.text_input("Nome file di output (CSV)", value="aziende_fotovoltaico_verificate.csv")
    start_button = st.button("🚀 Avvia verifica email", type="primary")
    log_box2 = st.empty()
    result_download2 = st.empty()

    if start_button:
        if not input_csv or not hunter_keys:
            st.error("Carica sia il CSV sia il file .txt con le API Key di Hunter.")
        else:
            tmp_dir2 = Path(tempfile.mkdtemp(prefix="email_verify_"))
            in_csv_path = tmp_dir2 / "input.csv"
            with open(in_csv_path, "wb") as f:
                f.write(input_csv.read())

            hunter_path = tmp_dir2 / "hunter_api_keys.txt"
            with open(hunter_path, "wb") as f:
                f.write(hunter_keys.read())

            # We'll run em_verification.py with args and simulate its interactive prompts:
            # It asks:
            #  - if to load backup -> reply "n"
            #  - then asks path to the API key file -> we pass our hunter_path
            # Also pass --input and --output via CLI.
            script2 = EMAIL_SCRIPT
            cmd2 = [
                PY_EXE, script2,
                "--input", str(in_csv_path),
                "--output", str(tmp_dir2 / out_name),
                "--accounts", "1"  # the script prints mail.tm accounts but we just supply keys file
            ]

            stdin_payload2 = f"n\n{hunter_path}\n"
            env2 = os.environ.copy()
            # If Tor is not in use, the script will still try; but it logs warnings.
            # You can change config later if you have Tor.

            st.info("Verifica avviata. Vedi log qui sotto.")

            logs2 = []
            for line in stream_subprocess(cmd2, cwd=str(tmp_dir2), env=env2, input_data=stdin_payload2):
                logs2.append(line.rstrip("\n"))
                if len(logs2) % 5 == 0:
                    log_box2.code("\n".join(logs2[-800:]))

            log_box2.code("\n".join(logs2[-1000:]))

            out_csv_path = tmp_dir2 / out_name
            if out_csv_path.exists() and out_csv_path.stat().st_size > 0:
                ts_name2 = f"verifica_{int(time.time())}.csv"
                final_path2 = output_dir / ts_name2
                try:
                    final_path2.write_bytes(out_csv_path.read_bytes())
                except Exception:
                    os.replace(str(out_csv_path), str(final_path2))

                st.success("Verifica completata! Scarica i risultati:")
                result_download2.download_button("⬇️ Scarica CSV", data=final_path2.read_bytes(), file_name=ts_name2, mime="text/csv")
                st.caption(f"Salvato anche in: `{final_path2}`")
            else:
                st.warning("Non ho trovato il CSV di output della verifica. Controlla i log.")

# -------------------- TAB 3: Guida --------------------
with tabs[2]:
    st.subheader("Guida rapida")
    st.markdown("""
**Come usare:**
1) Vai su **Sidebar → Impostazioni** e inserisci, se ce l'hai, le API key (Google Places e OpenAI).  
2) Nel tab **Scraping aziende**, incolla i comuni o carica un file con colonna `comune`. Clicca **Avvia**.  
3) Nel tab **Verifica email**, carica il CSV da verificare e il file **txt** con le API key di Hunter (una per riga). Clicca **Avvia**.  

**Dove trovo i risultati?**  
- Dopo ogni esecuzione comparirà un pulsante **Scarica CSV**.  
- Inoltre, tutto viene salvato nella cartella di output indicata nella sidebar.

**Note tecniche:**  
- La dashboard esegue i tuoi script originali come processi separati, gestendo gli input al posto tuo.  
- Se il tuo scraper usa metodi che richiedono browser/Selenium, assicurati di avere Chrome/Chromedriver correttamente installati.  
- Per Hunter.io: carica direttamente il file con le API key; la parte di registrazione mail.tm è opzionale e puoi ignorarla se hai già le key.
""")
