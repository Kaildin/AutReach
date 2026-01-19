import requests
import time
import random
import logging
import socket
import pandas as pd
import argparse
from bs4 import BeautifulSoup
from stem import Signal
from stem.control import Controller
from requests.exceptions import RequestException, ConnectionError, Timeout
from typing import Dict, List, Tuple, Optional, Any
import os
import json

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurazione (meglio spostare in un file di configurazione separato)
CONFIG = {
    "NUM_ACCOUNTS": 5,
    "TOR_PASSWORD": "",
    "TOR_CONTROL_PORT": 9051,
    "TOR_SOCKS_PORT": 9050,
    "MIN_DELAY": 2,
    "MAX_DELAY": 5,
    "HUNTER_API_URL": "https://api.hunter.io/v2/email-verifier",
    "CSV_INPUT_FILE": "Solar installer Leads-Scrapermio - Umbria1.csv",
    "CSV_OUTPUT_FILE": "aziende_fotovoltaico_verificate.csv",
    "API_BASE": "https://api.mail.tm",
    "MAX_RETRIES": 3,
    "RETRY_DELAY_BASE": 2  # Base per il backoff esponenziale
}

# -------------------------------
# Funzione di utility per gestire le richieste con retry
# -------------------------------
def request_with_retry(url: str, method: str = "get", max_retries: int = None, **kwargs) -> Optional[requests.Response]:
    """
    Effettua una richiesta HTTP con meccanismo di retry e backoff esponenziale.
    
    Args:
        url: URL per la richiesta
        method: Metodo HTTP (get, post, ecc.)
        max_retries: Numero massimo di tentativi
        **kwargs: Parametri aggiuntivi per la richiesta
        
    Returns:
        Response object o None in caso di fallimento
    """
    if max_retries is None:
        max_retries = CONFIG["MAX_RETRIES"]
        
    # Rimuovi 'session' dai kwargs se presente
    session = kwargs.pop('session', None)
    
    for attempt in range(max_retries):
        try:
            # Usa la sessione se fornita, altrimenti usa requests direttamente
            if session:
                if method.lower() == "get":
                    response = session.get(url, **kwargs)
                elif method.lower() == "post":
                    response = session.post(url, **kwargs)
                elif method.lower() == "delete":
                    response = session.delete(url, **kwargs)
            else:
                if method.lower() == "get":
                    response = requests.get(url, **kwargs)
                elif method.lower() == "post":
                    response = requests.post(url, **kwargs)
                elif method.lower() == "delete":
                    response = requests.delete(url, **kwargs)
                    
            # Verifica status code
            if response.status_code in [429, 503]:  # Rate limiting o servizio non disponibile
                wait_time = CONFIG["RETRY_DELAY_BASE"] ** (attempt + 1)
                logger.warning(f"Rate limit o server occupato. Attesa di {wait_time}s")
                time.sleep(wait_time)
                continue
                
            return response
            
        except ConnectionError as e:
            logger.warning(f"Errore di connessione (tentativo {attempt+1}/{max_retries}): {e}")
        except Timeout as e:
            logger.warning(f"Timeout (tentativo {attempt+1}/{max_retries}): {e}")
        except RequestException as e:
            logger.warning(f"Errore di richiesta (tentativo {attempt+1}/{max_retries}): {e}")
        except Exception as e:
            logger.warning(f"Errore imprevisto (tentativo {attempt+1}/{max_retries}): {e}")
            
        # Calcola tempo di attesa per backoff esponenziale
        wait_time = CONFIG["RETRY_DELAY_BASE"] ** attempt
        logger.info(f"Attesa di {wait_time}s prima del prossimo tentativo...")
        time.sleep(wait_time)
    
    logger.error(f"Tutti i {max_retries} tentativi falliti per {url}")
    return None

# -------------------------------
# Classe per la gestione della connessione Tor
# -------------------------------
class TorConnection:
    """Gestisce la connessione e la rotazione di IP tramite la rete Tor."""
    
    def __init__(self):
        """Inizializza la connessione con Tor."""
        self.max_retries = 3
        self.retry_delay = 5
        self._validate_tor_connection()
        
    def _validate_tor_connection(self) -> bool:
        """Verifica che Tor sia in esecuzione e accessibile."""
        try:
            with Controller.from_port(port=CONFIG["TOR_CONTROL_PORT"]) as controller:
                controller.authenticate(password=CONFIG["TOR_PASSWORD"])
                if controller.is_authenticated():
                    logger.info("Connessione a Tor stabilita e autenticata.")
                    return True
                else:
                    logger.error("Impossibile autenticarsi al controller Tor.")
                    return False
        except Exception as e:
            logger.error(f"Errore nella validazione della connessione Tor: {e}")
            logger.error("Assicurarsi che Tor sia in esecuzione e che la porta di controllo sia configurata correttamente.")
            return False

    def _get_controller(self):
        """Crea una nuova connessione al controller Tor."""
        try:
            controller = Controller.from_port(port=CONFIG["TOR_CONTROL_PORT"])
            controller.authenticate(password=CONFIG["TOR_PASSWORD"])
            return controller
        except Exception as e:
            logger.error(f"Errore nella connessione al controller Tor: {e}")
            return None

    def rotate_ip(self) -> bool:
        """Ruota l'IP utilizzando il controllo di Tor."""
        for attempt in range(self.max_retries):
            try:
                controller = self._get_controller()
                if not controller:
                    continue
                    
                with controller:
                    controller.signal(Signal.NEWNYM)
                    logger.info("IP ruotato tramite Tor.")
                    time.sleep(5)  # Attesa per la rotazione
                    return True
                    
            except Exception as e:
                logger.error(f"Tentativo {attempt + 1}/{self.max_retries} fallito: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error("Tutti i tentativi di rotazione IP falliti")
                    return False
            
    def get_current_ip(self) -> str:
        """
        Ottiene l'IP corrente utilizzato tramite Tor.
        
        Returns:
            str: Indirizzo IP corrente o messaggio di errore
        """
        try:
            # Utilizza un servizio che restituisce il tuo IP
            response = request_with_retry("https://api.ipify.org/?format=json", 
                                         proxies={'http': f'socks5://127.0.0.1:{CONFIG["TOR_SOCKS_PORT"]}',
                                                  'https': f'socks5://127.0.0.1:{CONFIG["TOR_SOCKS_PORT"]}'})
            if response and response.status_code == 200:
                return response.json().get('ip', 'Sconosciuto')
            return "Impossibile determinare l'IP"
        except Exception as e:
            logger.error(f"Errore nel recupero dell'IP: {e}")
            return "Errore nel recupero dell'IP"

# -------------------------------
# Classe per la gestione delle email su mail.tm
# -------------------------------
class MailTmManager:
    """Gestisce la creazione e l'accesso agli account email temporanei su mail.tm."""
    
    def __init__(self, session: requests.Session = None):
        """
        Inizializza il manager per mail.tm.
        
        Args:
            session: Sessione HTTP opzionale da utilizzare per le richieste
        """
        self.session = session or requests.Session()
        self.available_domains = self._get_available_domains()
        
    def _get_available_domains(self) -> List[str]:
        """
        Recupera i domini disponibili da mail.tm.
        
        Returns:
            List[str]: Lista dei domini disponibili o lista vuota in caso di errore
        """
        try:
            response = request_with_retry(f"{CONFIG['API_BASE']}/domains")
            if response and response.status_code == 200:
                domains_data = response.json()
                # Estrai i domini dalla risposta
                if isinstance(domains_data, dict) and 'hydra:member' in domains_data:
                    return [domain['domain'] for domain in domains_data['hydra:member']]
                elif isinstance(domains_data, list):
                    return [domain['domain'] for domain in domains_data]
            logger.warning("Impossibile recuperare i domini disponibili. Utilizzo dominio predefinito.")
            return ["mail.tm"]  # Dominio predefinito
        except Exception as e:
            logger.error(f"Errore nel recupero dei domini disponibili: {e}")
            return ["mail.tm"]  # Dominio predefinito in caso di errore

    def create_account(self) -> Dict[str, Any]:
        """
        Crea un account email temporaneo tramite mail.tm.
        
        Returns:
            Dict con i dettagli dell'account creato, inclusi email e token
        """
        if not self.available_domains:
            self.available_domains = self._get_available_domains()
            
        domain = random.choice(self.available_domains)
        username = f"user{random.randint(10000, 99999)}"
        email = f"{username}@{domain}"
        password = f"Password{random.randint(100000, 999999)}"
        
        account_data = {
            "address": email,
            "password": password
        }
        
        try:
            response = request_with_retry(
                f"{CONFIG['API_BASE']}/accounts", 
                method="post", 
                json=account_data,
                headers={"Content-Type": "application/json"}
            )
            
            if response and response.status_code in [200, 201]:
                account_info = response.json()
                # Ottieni il token di autenticazione
                token_response = request_with_retry(
                    f"{CONFIG['API_BASE']}/token",
                    method="post",
                    json={"address": email, "password": password},
                    headers={"Content-Type": "application/json"}
                )
                
                if token_response and token_response.status_code == 200:
                    token_data = token_response.json()
                    return {
                        "email": email,
                        "id": account_info.get("id", ""),
                        "token": token_data.get("token", ""),
                        "password": password
                    }
                    
            logger.error(f"Errore nella creazione dell'account: {response.status_code if response else 'No response'}")
            # Crea un account fittizio nel caso di errore
            return {"email": email, "id": "", "token": "", "password": ""}
            
        except Exception as e:
            logger.error(f"Errore nella creazione dell'account mail.tm: {e}")
            return {"email": email, "id": "", "token": "", "password": ""}

    def get_messages(self, email_account: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Recupera i messaggi per l'account specificato.
        
        Args:
            email_account: Dizionario con i dettagli dell'account
            
        Returns:
            Lista dei messaggi ricevuti
        """
        if not email_account.get("token"):
            logger.error("Token mancante per il recupero dei messaggi")
            return []
            
        try:
            headers = {
                "Authorization": f"Bearer {email_account['token']}",
                "Accept": "application/json"
            }
            
            response = request_with_retry(
                f"{CONFIG['API_BASE']}/messages",
                headers=headers
            )
            
            if response and response.status_code == 200:
                messages_data = response.json()
                if 'hydra:member' in messages_data:
                    return messages_data['hydra:member']
                return messages_data
                
            logger.error(f"Errore nel recupero dei messaggi: {response.status_code if response else 'No response'}")
            return []
            
        except Exception as e:
            logger.error(f"Errore nel recupero dei messaggi: {e}")
            return []
            
    def get_message_content(self, message_id: str, email_account: Dict[str, str]) -> Dict[str, Any]:
        """
        Recupera il contenuto di un messaggio specifico.
        
        Args:
            message_id: ID del messaggio da recuperare
            email_account: Dizionario con i dettagli dell'account
            
        Returns:
            Dizionario con i dettagli del messaggio
        """
        if not email_account.get("token"):
            logger.error("Token mancante per il recupero del contenuto del messaggio")
            return {}
            
        try:
            headers = {
                "Authorization": f"Bearer {email_account['token']}",
                "Accept": "application/json"
            }
            
            response = request_with_retry(
                f"{CONFIG['API_BASE']}/messages/{message_id}",
                headers=headers
            )
            
            if response and response.status_code == 200:
                return response.json()
                
            logger.error(f"Errore nel recupero del contenuto del messaggio: {response.status_code if response else 'No response'}")
            return {}
            
        except Exception as e:
            logger.error(f"Errore nel recupero del contenuto del messaggio: {e}")
            return {}
    
    def delete_account(self, email_account: Dict[str, str]) -> bool:
        """
        Elimina un account email temporaneo.
        
        Args:
            email_account: Dizionario con i dettagli dell'account
            
        Returns:
            True se l'eliminazione è avvenuta con successo, False altrimenti
        """
        if not email_account.get("id") or not email_account.get("token"):
            logger.error("ID o token mancante per l'eliminazione dell'account")
            return False
            
        try:
            headers = {
                "Authorization": f"Bearer {email_account['token']}",
                "Accept": "application/json"
            }
            
            response = request_with_retry(
                f"{CONFIG['API_BASE']}/accounts/{email_account['id']}",
                method="delete",
                headers=headers
            )
            
            if response and response.status_code in [200, 204]:
                logger.info(f"Account {email_account['email']} eliminato con successo.")
                return True
                
            logger.error(f"Errore nell'eliminazione dell'account: {response.status_code if response else 'No response'}")
            return False
            
        except Exception as e:
            logger.error(f"Errore nell'eliminazione dell'account: {e}")
            return False
    
    def random_delay(self):
        """Attende un periodo casuale tra le operazioni."""
        delay = random.uniform(CONFIG["MIN_DELAY"], CONFIG["MAX_DELAY"])
        logger.info(f"Attesa di {delay:.2f} secondi (MailTmManager)...")
        time.sleep(delay)

def load_hunter_api_keys(file_path="hunter_api_keys.txt"):
    try:
        with open(file_path, "r") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Errore nel caricamento delle API key Hunter.io: {e}")
        return []

# -------------------------------
# Classe per la verifica delle email con Hunter.io
# -------------------------------
class HunterEmailVerifier:
    """Gestisce la verifica delle email utilizzando l'API di Hunter.io."""
    
    def __init__(self, tor_connection: TorConnection, mail_manager: MailTmManager, api_keys: List[str]):
        """
        Inizializza il verificatore di email.
        
        Args:
            tor_connection: Istanza di TorConnection per la rotazione degli IP
            mail_manager: Istanza di MailTmManager per la gestione delle email temporanee
            api_keys: Lista delle API key di Hunter.io
        """
        self.tor_connection = tor_connection
        self.mail_manager = mail_manager
        self.current_account_index = 0
        self.hunter_accounts = [{"api_key": key, "usage_count": 0} for key in api_keys]
        self.verification_cache = {}  # Cache per evitare di riverificare le stesse email
        
    def random_delay(self):
        """Attende un periodo casuale tra le operazioni."""
        delay = random.uniform(CONFIG["MIN_DELAY"], CONFIG["MAX_DELAY"])
        logger.info(f"Attesa di {delay:.2f} secondi...")
        time.sleep(delay)

    def verify_email(self, email: str) -> Dict[str, Any]:
        """
        Verifica un indirizzo email usando Hunter.io.
        
        Args:
            email: Indirizzo email da verificare
            
        Returns:
            Dizionario con i risultati della verifica
        """
        # Verifica se l'email è già nella cache
        if email in self.verification_cache:
            logger.info(f"Email {email} già verificata, utilizzo dati in cache")
            return self.verification_cache[email]
            
        if not self.hunter_accounts:
            logger.error("Nessun account Hunter disponibile per la verifica")
            return {"result": "error", "status": "no_account_available"}

        # Seleziona l'account corrente
        account = self.hunter_accounts[self.current_account_index]

        # Se l'account ha raggiunto il limite di 50 verifiche, rimuovilo dalla pool
        if account.get("usage_count", 0) >= 50:
            logger.info(f"Account {account['api_key']} ha raggiunto il limite di 50 verifiche, lo rimuoviamo dalla pool")
            self.hunter_accounts.pop(self.current_account_index)
            if not self.hunter_accounts:
                logger.error("Nessun account Hunter disponibile per la verifica dopo aver raggiunto il limite")
                return {"result": "error", "status": "all_accounts_exhausted"}
            self.current_account_index %= len(self.hunter_accounts)
            account = self.hunter_accounts[self.current_account_index]

        # Ruota l'IP per la verifica
        self.tor_connection.rotate_ip()
        self.random_delay()

        try:
            params = {
                "email": email,
                "api_key": account["api_key"]
            }

            response = request_with_retry(CONFIG["HUNTER_API_URL"], params=params)

            # Incrementa il contatore di utilizzo per questo account
            account["usage_count"] = account.get("usage_count", 0) + 1
            
            # Passa al prossimo account per la prossima verifica
            self.current_account_index = (self.current_account_index + 1) % len(self.hunter_accounts)

            if response and response.status_code == 200:
                data = response.json()
                result = {
                    "result": "success",
                    "status": data.get("data", {}).get("status", "unknown"),
                    "score": data.get("data", {}).get("score", 0),
                    "sources": data.get("data", {}).get("sources", [])
                }
                # Salva nella cache
                self.verification_cache[email] = result
                return result
            elif response and response.status_code == 401:
                logger.warning(f"API key non valida per l'account {account['api_key']}")
                # Rimuovi questo account
                self.hunter_accounts.remove(account)
                if self.hunter_accounts:
                    return self.verify_email(email)
                else:
                    return {"result": "error", "status": "all_accounts_invalid"}
            else:
                error_status = f"api_error_{response.status_code}" if response else "connection_error"
                logger.error(f"Errore nella verifica dell'email {email}: {error_status}")
                return {"result": "error", "status": error_status}

        except Exception as e:
            logger.error(f"Errore durante la verifica dell'email {email}: {e}")
            return {"result": "error", "status": "exception", "message": str(e)}

# -------------------------------
# Funzioni per il caricamento e l'aggiornamento del CSV
# -------------------------------
def load_emails_from_csv(csv_file: str) -> Tuple[List[str], pd.DataFrame]:
    """
    Carica gli indirizzi email dal file CSV.
    
    Args:
        csv_file: Percorso del file CSV
        
    Returns:
        Tupla con la lista di email uniche e il DataFrame
    """
    try:
        if not os.path.exists(csv_file):
            logger.error(f"Il file CSV {csv_file} non esiste")
            return [], pd.DataFrame()
            
        df = pd.read_csv(csv_file)
        if "email" not in df.columns:
            logger.error(f"Colonna 'email' non trovata nel file CSV {csv_file}")
            return [], df
            
        emails = []
        for email_str in df["email"].dropna():
            email_list = [e.strip() for e in str(email_str).split(",") if e.strip()]
            emails.extend(email_list)
        
        # Filtra solo indirizzi email validi con una regex base
        valid_emails = []
        for email in emails:
            if "@" in email and "." in email.split("@")[1]:
                valid_emails.append(email)
            else:
                logger.warning(f"Email ignorata perché non valida: {email}")
                
        unique_emails = list(set(valid_emails))
        logger.info(f"Caricate {len(unique_emails)} email uniche valide dal file CSV")
        return unique_emails, df
        
    except pd.errors.EmptyDataError:
        logger.error(f"Il file CSV {csv_file} è vuoto")
        return [], pd.DataFrame()
    except pd.errors.ParserError:
        logger.error(f"Errore di parsing del file CSV {csv_file}")
        return [], pd.DataFrame()
    except Exception as e:
        logger.error(f"Errore nel caricamento delle email dal CSV: {e}")
        return [], pd.DataFrame()



def update_csv_with_verification(df: pd.DataFrame, email_verifications: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    """
    Aggiorna il DataFrame con i risultati della verifica.
    
    Args:
        df: DataFrame da aggiornare
        email_verifications: Dizionario con i risultati della verifica
        
    Returns:
        DataFrame aggiornato
    """
    # Crea nuove colonne se non esistono
    for col in ["email_verificata", "email_score", "email_status"]:
        if col not in df.columns:
            df[col] = ""

    # Iterazione ottimizzata sul DataFrame
    for index, row in df.iterrows():
        email_str = row.get("email", "")
        if pd.isna(email_str) or not email_str:
            continue
            
        email_list = [e.strip() for e in str(email_str).split(",")]
        verified_emails = []
        scores = []
        statuses = []

        for email in email_list:
            if email in email_verifications:
                result = email_verifications[email]
                if result["result"] == "success":
                    verified_emails.append(email)
                    scores.append(str(result.get("score", 0)))
                    statuses.append(result.get("status", "unknown"))

        df.at[index, "email_verificata"] = ", ".join(verified_emails) if verified_emails else ""
        df.at[index, "email_score"] = ", ".join(scores) if scores else ""
        df.at[index, "email_status"] = ", ".join(statuses) if statuses else ""

    return df

def save_csv(df: pd.DataFrame, output_file: str) -> bool:
    """
    Salva il DataFrame in un file CSV.
    
    Args:
        df: DataFrame da salvare
        output_file: Percorso del file di output
        
    Returns:
        True se il salvataggio è avvenuto con successo, False altrimenti
    """
    try:
        df.to_csv(output_file, index=False)
        logger.info(f"File CSV aggiornato salvato come {output_file}")
        return True
    except Exception as e:
        logger.error(f"Errore nel salvataggio del file CSV: {e}")
        return False

# -------------------------------
# Funzione per backup periodico dei risultati
# -------------------------------
def backup_results(email_verifications: Dict[str, Dict[str, Any]], backup_file: str = "email_verifications_backup.json") -> bool:
    """
    Salva un backup dei risultati della verifica.
    
    Args:
        email_verifications: Dizionario con i risultati della verifica
        backup_file: Percorso del file di backup
        
    Returns:
        True se il backup è avvenuto con successo, False altrimenti
    """
    try:
        with open(backup_file, 'w') as f:
            json.dump(email_verifications, f)
        logger.info(f"Backup dei risultati salvato in {backup_file}")
        return True
    except Exception as e:
        logger.error(f"Errore nel salvataggio del backup: {e}")
        return False

def load_backup(backup_file: str = "email_verifications_backup.json") -> Dict[str, Dict[str, Any]]:
    """
    Carica il backup dei risultati della verifica.
    
    Args:
        backup_file: Percorso del file di backup
        
    Returns:
        Dizionario con i risultati della verifica o dizionario vuoto in caso di errore
    """
    try:
        if os.path.exists(backup_file):
            with open(backup_file, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Errore nel caricamento del backup: {e}")
        return {}

# -------------------------------
# Funzione principale
# -------------------------------
def main():
    """Funzione principale del programma."""
    parser = argparse.ArgumentParser(description="Email Verification System")
    parser.add_argument("--input", type=str, default=CONFIG["CSV_INPUT_FILE"], help="Input CSV file with emails")
    parser.add_argument("--output", type=str, default=CONFIG["CSV_OUTPUT_FILE"], help="Output CSV file with verified emails")
    parser.add_argument("--accounts", type=int, default=CONFIG["NUM_ACCOUNTS"], help="Number of Hunter accounts to create")
    parser.add_argument("--backup", type=str, default="email_verifications_backup.json", help="Backup file for verification results")
    parser.add_argument("--tor-control-port", type=int, default=CONFIG["TOR_CONTROL_PORT"], help="Tor control port")
    parser.add_argument("--tor-socks-port", type=int, default=CONFIG["TOR_SOCKS_PORT"], help="Tor SOCKS port")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Aggiorna la configurazione con i parametri della riga di comando
    CONFIG["CSV_INPUT_FILE"] = args.input
    CONFIG["CSV_OUTPUT_FILE"] = args.output
    CONFIG["NUM_ACCOUNTS"] = args.accounts
    CONFIG["TOR_CONTROL_PORT"] = args.tor_control_port
    CONFIG["TOR_SOCKS_PORT"] = args.tor_socks_port
    
    # Imposta il livello di logging
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Modalità debug attivata")

    try:
        # Verifica se esiste un backup e chiedi se caricarlo
        email_verifications = {}
        if os.path.exists(args.backup):
            response = input(f"Trovato un backup in {args.backup}. Vuoi caricarlo? (s/n): ")
            if response.lower() in ["s", "si", "sì", "y", "yes"]:
                email_verifications = load_backup(args.backup)
                logger.info(f"Caricati {len(email_verifications)} risultati dal backup")
        
        logger.info("Inizializzazione connessione Tor...")
        tor_connection = TorConnection()

        # Crea una sessione per le richieste HTTP
        session = requests.Session()
        
        # Configura la sessione per utilizzare Tor come proxy
        session.proxies = {
            'http': f'socks5://127.0.0.1:{CONFIG["TOR_SOCKS_PORT"]}',
            'https': f'socks5://127.0.0.1:{CONFIG["TOR_SOCKS_PORT"]}'
        }

        # Inizializza il gestore di mail.tm
        mail_manager = MailTmManager(session)

        # Genera email temporanee e stampale
        num_accounts = CONFIG["NUM_ACCOUNTS"]
        mail_accounts = []
        print("\n=== EMAIL TEMPORANEE DA USARE PER LA REGISTRAZIONE SU HUNTER.IO ===")
        for i in range(num_accounts):
            acc = mail_manager.create_account()
            mail_accounts.append(acc)
            print(f"{i+1}) Email: {acc['email']} | Password: {acc['password']}")
        print("====================================================================\n")
        print("Usa queste email per registrare manualmente gli account su Hunter.io.")
        print("Dopo aver ottenuto le API key, inseriscile in un file (una per riga).")

        # Chiedi il percorso del file delle API key
        api_key_file = input("Inserisci il percorso del file con le API key di Hunter.io (es: hunter_api_keys.txt): ").strip()
        api_keys = load_hunter_api_keys(api_key_file)
        if not api_keys:
            logger.error("Nessuna API key Hunter.io trovata. Inseriscile nel file e riprova.")
            return

        verifier = HunterEmailVerifier(tor_connection, mail_manager, api_keys)

        # Carica le email dal CSV
        emails, df = load_emails_from_csv(CONFIG["CSV_INPUT_FILE"])
        if not emails:
            logger.error("Nessuna email trovata da verificare.")
            return
            
        if df.empty:
            logger.error("DataFrame vuoto dopo il caricamento del CSV.")
            return

        # Filtra le email già verificate
        emails_to_verify = [email for email in emails if email not in email_verifications]
        logger.info(f"Email da verificare: {len(emails_to_verify)} di {len(emails)}")

        # Backup periodico dei risultati
        backup_interval = 10  # Backup ogni 10 email verificate
        
        # Verifica le email
        for i, email in enumerate(emails_to_verify):
            logger.info(f"Verifica email {i+1}/{len(emails_to_verify)}: {email}")
            result = verifier.verify_email(email)
            email_verifications[email] = result

            if result["result"] == "success":
                logger.info(f"Email {email}: Status = {result['status']}, Score = {result['score']}")
            else:
                logger.warning(f"Email {email}: Errore - {result['status']}")
                
            # Backup periodico
            if (i + 1) % backup_interval == 0:
                backup_results(email_verifications, args.backup)
                
            # Aggiorna e salva il CSV ogni 20 email per evitare perdita di dati
            if (i + 1) % 20 == 0:
                updated_df = update_csv_with_verification(df.copy(), email_verifications)
                save_csv(updated_df, CONFIG["CSV_OUTPUT_FILE"])

        # Aggiorna il CSV con i risultati finali
        updated_df = update_csv_with_verification(df, email_verifications)
        save_csv(updated_df, CONFIG["CSV_OUTPUT_FILE"])
        
        # Backup finale
        backup_results(email_verifications, args.backup)

        # Riepilogo finale
        logger.info("=" * 50)
        logger.info("RIEPILOGO VERIFICHE EMAIL:")
        success_count = sum(1 for r in email_verifications.values() if r["result"] == "success")
        error_count = len(email_verifications) - success_count
        logger.info(f"Email verificate con successo: {success_count}")
        logger.info(f"Email con errori: {error_count}")
        logger.info(f"CSV con risultati salvato in: {CONFIG['CSV_OUTPUT_FILE']}")
        logger.info(f"Backup dei risultati salvato in: {args.backup}")

    except KeyboardInterrupt:
        logger.info("Interruzione manuale del programma")
        if 'df' in locals() and 'email_verifications' in locals():
            # Salva lo stato attuale prima di uscire
            updated_df = update_csv_with_verification(df, email_verifications)
            save_csv(updated_df, CONFIG["CSV_OUTPUT_FILE"])
            backup_results(email_verifications, args.backup)
            logger.info("Stato salvato prima dell'uscita")
    except Exception as e:
        logger.error(f"Errore nell'esecuzione del programma: {e}")
        if 'df' in locals() and 'email_verifications' in locals():
            # Tenta di salvare lo stato in caso di errore
            try:
                updated_df = update_csv_with_verification(df, email_verifications)
                save_csv(updated_df, f"emergency_{CONFIG['CSV_OUTPUT_FILE']}")
                backup_results(email_verifications, f"emergency_{args.backup}")
                logger.info("Stato di emergenza salvato")
            except:
                logger.critical("Impossibile salvare lo stato di emergenza")

if __name__ == "__main__":
    # Assicurati che tutte le librerie necessarie siano importate
    main()