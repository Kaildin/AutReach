import os
import sys
import csv
import time
import argparse
import logging
import random
from typing import Dict, List, Optional

# Permette l'esecuzione come file: python src/outreach_saas/pipelines/admin_pipeline.py
if __package__ is None or __package__ == "":
    # aggiunge "src" al PYTHONPATH
    this_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.abspath(os.path.join(this_dir, "..", ".."))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from outreach_saas.scraping.driver_utils import init_driver_helper, cleanup_chrome_tmp
from outreach_saas.scraping.search_utils import test_single_query, ddg_search_snippets, admin_from_ddg_with_gpt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def _read_csv_rows(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV non trovato: {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames or []


def _write_csv_rows(path: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _looks_like_captcha(driver) -> bool:
    try:
        src = (driver.page_source or "").lower()
        return ("captcha" in src) or ("recaptcha" in src) or ("g-recaptcha" in src)
    except Exception:
        return False


def run_admin_pipeline(
    input_csv: str,
    output_csv: str,
    headless: bool = True,
    restart_every: int = 20,
    max_items: Optional[int] = None,
    force: bool = False,
    base_sleep_min: float = 2.0,
    base_sleep_max: float = 4.0,
):
    (rows, fieldnames) = _read_csv_rows(input_csv)

    if not rows:
        logger.warning("CSV input vuoto. Fine.")
        return

    # Assicura colonne target
    required_cols = ["nome", "contatto"]
    for c in required_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    # Colonne extra di debug/telemetria admin (utili per capire captcha/qualità)
    extra_cols = [
        "admin_query",
        "admin_status",
        "admin_notes",
        "admin_last_run_ts",
        "admin_name",         # se vuoi separato da contatto
        "ddg_top_urls",
        "ddg_payload_preview"
    ]
    for c in extra_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    driver = None
    processed = 0
    captcha_hits = 0

    def start_driver():
        nonlocal driver
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        driver = init_driver_helper(headless=headless)
        return driver

    try:
        start_driver()

        for idx, r in enumerate(rows):
            if max_items is not None and processed >= max_items:
                break

            nome = (r.get("nome") or "").strip()
            if not nome:
                r["admin_status"] = "skip"
                r["admin_notes"] = "nome vuoto"
                continue

            already = (r.get("contatto") or "").strip()
            if already and not force:
                r["admin_status"] = "skip"
                r["admin_notes"] = "contatto già presente"
                continue

            # restart periodico per “pulizia sessione”
            if restart_every > 0 and processed > 0 and processed % restart_every == 0:
                logger.info(f"[restart] Restart driver dopo {processed} query")
                start_driver()
                cleanup_chrome_tmp()

            query = f"{nome} amministratore"
            r["admin_query"] = query
            r["admin_last_run_ts"] = str(int(time.time()))

            admin_name = None
            ddg_payload = ""
            ddg_results = []

            try:
                admin_name, ddg_payload, ddg_results = admin_from_ddg_with_gpt(nome, query, max_results=5)
            except Exception as e:
                r["admin_status"] = "error"
                r["admin_notes"] = f"ddg_or_gpt_exception: {e}"
                admin_name = None
                ddg_payload = ""
                ddg_results = []

            # salva sempre i risultati DDG (se presenti)
            if ddg_results:
                r["ddg_top_urls"] = " | ".join([it.get("url","") for it in ddg_results[:5] if it.get("url")])
            else:
                # se DDG è bloccato/202, ddg_search_snippets ritorna [] nel tuo codice
                # quindi lo distinguiamo come "ddg_blocked" se vuoi:
                r["ddg_top_urls"] = ""

            r["ddg_payload_preview"] = (ddg_payload[:800].replace("\n", " ") if ddg_payload else "")

            if admin_name:
                r["admin_name"] = admin_name
                r["contatto"] = admin_name  # se vuoi mantenere compatibilità
                r["admin_status"] = "ok"
                r["admin_notes"] = "ddg->gpt"
                logger.info(f"  -> admin: {admin_name}")
            else:
                # se DDG ritorna vuoto spesso è 202/softblock o nessun risultato
                # distinguiamo:
                if not ddg_results:
                    r["admin_status"] = "ddg_empty_or_blocked"
                    r["admin_notes"] = "DDG vuoto (possibile 202/rate-limit o zero risultati)"
                else:
                    r["admin_status"] = "not_found"
                    r["admin_notes"] = "GPT non ha estratto un nome dai risultati DDG"

            processed += 1


        _write_csv_rows(output_csv, rows, fieldnames)
        logger.info(f"Fatto. Salvato: {output_csv}")
        logger.info(f"Query processate: {processed} | CAPTCHA: {captcha_hits}")

    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        cleanup_chrome_tmp()


def main():
    p = argparse.ArgumentParser(description="Admin Pipeline (solo ricerca amministratore)")
    p.add_argument("--input", required=True, help="CSV output della main_pipeline")
    p.add_argument("--output", default="output/admin_enriched.csv", help="CSV finale con colonna contatto aggiornata")
    p.add_argument("--headless", action="store_true", help="Esegui Chrome headless")
    p.add_argument("--no-headless", action="store_true", help="Disabilita headless (debug)")
    p.add_argument("--restart-every", type=int, default=20, help="Restart driver ogni N query")
    p.add_argument("--limit", type=int, default=0, help="Processa solo N righe (0 = tutte)")
    p.add_argument("--force", action="store_true", help="Ricalcola anche se contatto già presente")
    args = p.parse_args()

    headless = True
    if args.no_headless:
        headless = False
    elif args.headless:
        headless = True

    max_items = None if args.limit == 0 else args.limit

    run_admin_pipeline(
        input_csv=args.input,
        output_csv=args.output,
        headless=headless,
        restart_every=args.restart_every,
        max_items=max_items,
        force=args.force,
    )


if __name__ == "__main__":
    main()
