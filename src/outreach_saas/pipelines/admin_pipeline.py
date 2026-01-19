import os
import sys
import csv
import time
import argparse
import logging
import random
from typing import Dict, List, Optional

# Permette l'esecuzione come file
if __package__ is None or __package__ == "":
    this_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.abspath(os.path.join(this_dir, "..", ".."))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

from outreach_saas.scraping.driver_utils import cleanup_chrome_tmp
from outreach_saas.scraping.search_utils import extract_admin_with_gpt

# NUOVO: Import del searcher migliorato
from outreach_saas.scraping.ddg_search_improved import DDGSearcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def _read_csv_rows(path: str) -> tuple[List[Dict[str, str]], List[str]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input CSV non trovato: {path}")
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), list(reader.fieldnames or [])


def _write_csv_rows(path: str, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_gpt_payload(company_name: str, query: str, ddg_results: list, max_items: int = 5) -> str:
    """Crea payload per GPT dai risultati DDG"""
    lines = [
        f"AZIENDA: {company_name}",
        f"QUERY: {query}",
        "",
        "RISULTATI (DuckDuckGo):"
    ]
    
    for i, item in enumerate(ddg_results[:max_items], 1):
        title = (item.get("title") or "").strip()
        snippet = (item.get("snippet") or "").strip()
        url = (item.get("url") or "").strip()
        
        lines.append(f"\n[{i}] TITOLO: {title}")
        lines.append(f"[{i}] SNIPPET: {snippet}")
        lines.append(f"[{i}] URL: {url}")
    
    return "\n".join(lines).strip()


def run_admin_pipeline(
    input_csv: str,
    output_csv: str,
    restart_every: int = 20,
    max_items: Optional[int] = None,
    force: bool = False,
    base_sleep_min: float = 10.0,  # Aumentato
    base_sleep_max: float = 15.0,  # Aumentato
):
    """
    Pipeline amministratori con DDG migliorato (no Selenium)
    """
    (rows, fieldnames) = _read_csv_rows(input_csv)

    if not rows:
        logger.warning("CSV input vuoto. Fine.")
        return

    # Assicura colonne necessarie
    required_cols = ["nome", "contatto"]
    for c in required_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    # Colonne di debug/telemetria
    extra_cols = [
        "admin_query",
        "admin_status",
        "admin_notes",
        "admin_last_run_ts",
        "admin_name",
        "ddg_top_urls",
        "ddg_payload_preview",
        "ddg_delay_used"  # Traccia il delay per debug
    ]
    for c in extra_cols:
        if c not in fieldnames:
            fieldnames.append(c)

    # INIZIALIZZA IL SEARCHER (mantiene stato tra query)
    ddg = DDGSearcher()
    
    processed = 0
    success_count = 0
    ddg_blocked_count = 0

    try:
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

            # Costruisci query
            query = f"{nome} amministratore"
            r["admin_query"] = query
            r["admin_last_run_ts"] = str(int(time.time()))

            logger.info(f"\n[{processed+1}/{len(rows)}] Elaborazione: {nome}")

            admin_name = None
            ddg_payload = ""
            ddg_results = []

            try:
                # RICERCA CON DDG MIGLIORATO
                logger.info(f"[DDG] Searching for: {query}")
                ddg_results = ddg.search(query, max_results=5)
                
                # Traccia il delay corrente (utile per debug)
                r["ddg_delay_used"] = f"{ddg.current_delay:.1f}s"
                
                if not ddg_results:
                    r["admin_status"] = "ddg_empty_or_blocked"
                    r["admin_notes"] = "DDG non ha restituito risultati (possibile rate limit)"
                    ddg_blocked_count += 1
                    
                    # Se siamo bloccati troppo spesso, aumenta il delay base
                    if ddg_blocked_count >= 3:
                        logger.warning(f"[DDG] {ddg_blocked_count} blocchi consecutivi. Pausa extra...")
                        time.sleep(random.uniform(30, 45))
                        ddg_blocked_count = 0  # Reset counter
                    
                    continue

                # Reset counter se abbiamo successo
                ddg_blocked_count = 0

                # Costruisci payload per GPT
                ddg_payload = build_gpt_payload(nome, query, ddg_results, max_items=5)
                
                # Salva info DDG
                r["ddg_top_urls"] = " | ".join([
                    it.get("url", "")[:100] for it in ddg_results[:5] if it.get("url")
                ])
                r["ddg_payload_preview"] = (
                    ddg_payload[:500].replace("\n", " ") if ddg_payload else ""
                )

                # LOG: Testo inviato a GPT
                logger.info("=== PAYLOAD → GPT (START) ===")
                logger.info(ddg_payload[:2000])
                logger.info("=== PAYLOAD → GPT (END) ===")

                # ESTRAZIONE CON GPT
                admin_name = extract_admin_with_gpt(ddg_payload)

                if admin_name:
                    r["admin_name"] = admin_name
                    r["contatto"] = admin_name
                    r["admin_status"] = "ok"
                    r["admin_notes"] = "ddg->gpt"
                    success_count += 1
                    logger.info(f"✓ Admin trovato: {admin_name}")
                else:
                    r["admin_status"] = "not_found"
                    r["admin_notes"] = "GPT non ha estratto un nome dai risultati DDG"
                    logger.warning(f"✗ Nessun admin estratto")

            except Exception as e:
                logger.error(f"[ERROR] Eccezione per {nome}: {e}", exc_info=True)
                r["admin_status"] = "error"
                r["admin_notes"] = f"exception: {str(e)[:200]}"

            processed += 1
            
            # Salvataggio intermedio ogni 5 righe
            if processed % 5 == 0:
                _write_csv_rows(output_csv, rows, fieldnames)
                logger.info(f"[CHECKPOINT] Salvato a: {output_csv}")

        # Salvataggio finale
        _write_csv_rows(output_csv, rows, fieldnames)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"PIPELINE COMPLETATA")
        logger.info(f"{'='*60}")
        logger.info(f"Output salvato: {output_csv}")
        logger.info(f"Query processate: {processed}")
        logger.info(f"Admin trovati: {success_count}/{processed}")
        logger.info(f"Tasso successo: {(success_count/processed*100):.1f}%")
        logger.info(f"Delay finale DDG: {ddg.current_delay:.1f}s")
        logger.info(f"{'='*60}\n")

    finally:
        cleanup_chrome_tmp()


def main():
    p = argparse.ArgumentParser(
        description="Admin Pipeline - Ricerca amministratori con DDG migliorato"
    )
    p.add_argument("--input", required=True, help="CSV output della main_pipeline")
    p.add_argument(
        "--output", 
        default="output/admin_enriched.csv", 
        help="CSV finale con colonna contatto"
    )
    p.add_argument(
        "--limit", 
        type=int, 
        default=0, 
        help="Processa solo N righe (0 = tutte)"
    )
    p.add_argument(
        "--force", 
        action="store_true", 
        help="Ricalcola anche se contatto già presente"
    )
    p.add_argument(
        "--restart-every",
        type=int,
        default=0,
        help="Parametro deprecato (non più necessario senza Selenium)"
    )
    
    args = p.parse_args()

    max_items = None if args.limit == 0 else args.limit

    run_admin_pipeline(
        input_csv=args.input,
        output_csv=args.output,
        restart_every=args.restart_every,
        max_items=max_items,
        force=args.force,
    )


if __name__ == "__main__":
    main()