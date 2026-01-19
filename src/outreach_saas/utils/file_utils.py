import csv
import os
import shutil
import logging
import time
from .text_utils import clean_url, clean_extracted_text
import re

logger = logging.getLogger(__name__)

def ensure_csv_header(output_file, fieldnames):
    """Assicura che il file CSV abbia l'intestazione corretta"""
    file_exists = os.path.exists(output_file)
    if not file_exists:
        try:
            # Assicura che la directory esista
            os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
            with open(output_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
        except Exception as e:
            logger.error(f"Errore nella creazione header CSV {output_file}: {e}")
    else:
        # Se il file esiste, assicuriamoci che finisca con un newline prima di appendere
        try:
            with open(output_file, mode='rb+') as file:
                file.seek(0, os.SEEK_END)
                if file.tell() > 0:
                    file.seek(-1, os.SEEK_END)
                    last_char = file.read(1)
                    if last_char != b'\n' and last_char != b'\r':
                        file.write(b'\n')
        except Exception as e:
            logger.debug(f"Impossibile verificare newline finale di {output_file}: {e}")

def save_to_csv(results, output_file):
    """Salva una lista di risultati in un file CSV (sovrascrive)"""
    if not results:
        logger.warning("Nessun risultato da salvare.")
        return
    
    # Determina i campi (assumiamo che tutti i dict abbiano le stesse chiavi o un set noto)
    if not results: 
        return
        
    # Unione di tutte le chiavi possibili per sicurezza
    fieldnames = set()
    for r in results:
        fieldnames.update(r.keys())
    fieldnames = list(fieldnames)
    
    # Ordiniamo fieldnames preferibilmente con un ordine logico se possibile
    priority_fields = ['comune', 'keyword', 'nome', 'sito_web', 'telefono', 'email', 'pertinenza']
    fieldnames.sort(key=lambda x: priority_fields.index(x) if x in priority_fields else 999)

    try:
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Risultati salvati in {output_file}")
    except Exception as e:
        logger.error(f"Errore nel salvataggio CSV {output_file}: {e}")

def create_backup(output_file):
    """Crea una copia di backup del file"""
    try:
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            backup_file = output_file.replace('.csv', '_backup.csv')
            shutil.copy2(output_file, backup_file)
            logger.debug(f"Backup creato: {backup_file}")
    except Exception as e:
        logger.warning(f"Impossibile creare backup: {e}")

def save_final_backup(results, output_file):
    """Salva un backup finale di tutti i risultati"""
    try:
        if results:
            backup_file = output_file.replace('.csv', '_final_backup.csv')
            save_to_csv(results, backup_file)
            logger.info(f"Backup finale salvato: {backup_file}")
    except Exception as e:
        logger.warning(f"Impossibile creare backup finale: {e}")

def load_existing_keys(output_file):
    """
    Carica le chiavi (nome, comune, sito) dei risultati già presenti nel CSV
    per evitare duplicati durante il resume.
    """
    keys = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Chiave composita normalizzata
                    key = (
                        str(row.get('nome','')).strip().lower(),
                        str(row.get('comune','')).strip().lower(),
                        clean_url(str(row.get('sito_web','')).strip().lower()) if row.get('sito_web') else ''
                    )
                    keys.add(key)
        except Exception as e:
            logger.error(f"Impossibile caricare chiavi esistenti da {output_file}: {e}")
    return keys

def append_result_to_csv(result, output_file, fieldnames):
    """Appende un risultato al CSV con gestione errori e backup periodico"""
    try:
        ensure_csv_header(output_file, fieldnames)
        with open(output_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerow(result)
            
        # Gestione stato backup (usiamo attributo funzione per semplicità)
        if not hasattr(append_result_to_csv, 'count'):
            append_result_to_csv.count = 0
        
        append_result_to_csv.count += 1
        
        if append_result_to_csv.count % 10 == 0:
            create_backup(output_file)
            
    except Exception as e:
        logger.error(f"Errore nel salvataggio del risultato in {output_file}: {e}")
        # Emergency backup
        emergency_file = output_file.replace('.csv', '_emergency_backup.csv')
        try:
            ensure_csv_header(emergency_file, fieldnames)
            with open(emergency_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writerow(result)
        except Exception as e2:
            logger.critical(f"Errore critico nel salvataggio di emergenza: {e2}")

def append_api_usage_log(counters, log_dir="logs", log_filename="places_api_usage.csv"):
    """Logga l'utilizzo delle API"""
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
                logger.warning(f"Impossibile leggere log esistente per cumulativo: {e}")

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
    except Exception as e:
        logger.error(f"Errore nella scrittura del log di utilizzo API: {e}")
