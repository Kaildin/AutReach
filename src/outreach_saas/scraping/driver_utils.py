import logging
import os
import shutil
import subprocess
import re

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import undetected_chromedriver as uc

logger = logging.getLogger(__name__)

def init_driver_helper(headless=True):
    """Helper centralizzato per inizializzare il driver Chrome."""
    logger.info(f"Inizializzazione driver Chrome (headless={headless})...")
    
    chrome_options = Options()
    
    # Opzioni base
    if headless:
        chrome_options.add_argument("--headless=new")
    
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--log-level=3")
    
    # Opzioni Linux/Snap
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-background-networking")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-breakpad")
    chrome_options.add_argument("--disable-component-extensions-with-background-pages")
    chrome_options.add_argument("--disable-features=TranslateUI")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-sync")
    chrome_options.add_argument("--metrics-recording-only")
    chrome_options.add_argument("--mute-audio")
    chrome_options.add_argument("--no-default-browser-check")
    chrome_options.add_argument("--no-first-run")
    chrome_options.add_argument("--safebrowsing-disable-auto-update")
    chrome_options.add_argument("--enable-automation")
    chrome_options.add_argument("--password-store=basic")
    chrome_options.add_argument("--use-mock-keychain")
    
    # Opzioni stealth
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = None
    
    try:
        # Prova a trovare il percorso di Chromium su Linux
        chromium_paths = [
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/snap/bin/chromium",
            shutil.which("chromium-browser"),
            shutil.which("chromium"),
            shutil.which("google-chrome")
        ]
        chromium_binary = None
        for path in chromium_paths:
            if path and os.path.exists(path):
                chromium_binary = path
                logger.info(f"Trovato Chromium/Chrome in: {chromium_binary}")
                break
        
        # Prova a ottenere la versione di Chromium per forzare ChromeDriver corretto
        chromium_version = None
        chromium_version_int = None
        if chromium_binary:
            try:
                result = subprocess.run([chromium_binary, "--version"], capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    match = re.search(r'(\d+)\.\d+\.\d+\.\d+', result.stdout)
                    if match:
                        chromium_version = match.group(1)
                        chromium_version_int = int(chromium_version)
                        logger.info(f"Versione Chromium rilevata: {chromium_version}")
            except Exception as e:
                logger.warning(f"Impossibile rilevare versione Chromium: {e}")
        
        if chromium_binary:
            chrome_options.binary_location = chromium_binary
        
        # Tentativo 1: undetected_chromedriver
        logger.info(f"Tentativo con undetected_chromedriver (version_main={chromium_version_int})...")
        try:
            uc_options = Options()
            if headless:
                uc_options.add_argument("--headless=new")
            uc_options.add_argument("--no-sandbox")
            uc_options.add_argument("--disable-dev-shm-usage")
            uc_options.add_argument("--disable-gpu")
            if chromium_binary:
                uc_options.binary_location = chromium_binary
            
            driver = uc.Chrome(options=uc_options, version_main=chromium_version_int if chromium_version_int else None)
            logger.info("Chrome avviato con successo usando undetected_chromedriver")
            return driver
        except Exception as uc_error:
            logger.warning(f"undetected_chromedriver fallito: {uc_error}, provo con ChromeDriverManager...")
        
        # Tentativo 2: ChromeDriverManager
        service = webdriver.ChromeService(
            ChromeDriverManager(
                chrome_type=ChromeType.CHROMIUM if "chromium" in (chromium_binary or "").lower() else ChromeType.GOOGLE,
                driver_version=chromium_version if chromium_version else None
            ).install()
        )
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'})
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("Chrome avviato con ChromeDriverManager")
        return driver

    except Exception as e:
        logger.error(f"Errore fatale inizializzazione driver: {e}")
        if driver:
            try:
                driver.quit()
            except:
                pass
        raise

def cleanup_chrome_tmp():
    """Pulisce le directory temporanee create da Chrome/Selenium"""
    import glob
    try:
        # Pattern comuni per directory temporanee di Chrome/Selenium
        tmp_patterns = [
            "/tmp/.org.chromium.Chromium.*",
            "/tmp/.com.google.Chrome.*",
            "/tmp/scoped_dir*",
            "/tmp/chrome_BITS_*",
            "/tmp/undetected_chromedriver*"
        ]
        
        count = 0
        for pattern in tmp_patterns:
            for path in glob.glob(pattern):
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        os.remove(path)
                    count += 1
                except Exception:
                    pass
                    
        if count > 0:
            logger.info(f"Pulizia completata: rimosse {count} directory temporanee di Chrome")
            
    except Exception as e:
        logger.warning(f"Errore durante la pulizia dei file temporanei: {e}")
