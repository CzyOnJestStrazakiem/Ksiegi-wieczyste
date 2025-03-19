import asyncio
import argparse
import json
import os
import random
import time
from pathlib import Path
import logging
from playwright.async_api import async_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description="Pobieranie danych przez proxy")
parser.add_argument("-workers", type=int, help="Liczba workerów")
parser.add_argument("-in", dest="ksiegi_file", type=str, help="Plik ksiąg")
parser.add_argument("--ip-verification", choices=["enabled", "disabled"], default=None)
args = parser.parse_args()

with open("settings.json", "r", encoding="utf-8") as f:
    settings = json.load(f)

if args.ip_verification is not None:
    settings["ip_verification_enabled"] = (args.ip_verification == "enabled")
if args.workers:
    settings["workers"] = args.workers
if args.ksiegi_file:
    settings["ksiegi_file"] = args.ksiegi_file
    prefix = args.ksiegi_file.split()[0]
    settings["plik_wyjsciowy"] = f"{prefix}.txt"
    settings["error_log"] = f"{prefix}_errors.log"
    settings["proxy_error_log"] = f"{prefix}_proxy_errors.log"

IP_VERIFICATION_ENABLED = settings.get("ip_verification_enabled", True)
OUTPUT_FILE = settings["plik_wyjsciowy"]
ERROR_LOG = settings["error_log"]
PROXY_ERROR_LOG = settings["proxy_error_log"]
WORKERS = settings["workers"]
HEADLESS = settings["headless"]
BROWSER_TYPE = settings["browser"]
PROXY_ENABLED = settings["browser_options"].get("proxy", False)
USER_AGENTS = settings["browser_options"].get("user_agent_list", [])
LOCALES = settings["browser_options"].get("locale", [])
PROXY_LIST_FILE = settings.get("proxy_list", "")
PROXY_PROTOCOL = settings["browser_options"].get("proxy_protocol", "http")
BROWSER_ARGS = settings.get("browser_args", [])
LOGGING_ENABLED = settings.get("logging_enabled", True)
LOGGING_PROXY_ENABLED = settings.get("logging_proxy_enabled", True)
PROXY_ERRORLOGGING_ENABLED = settings.get("proxy_errorlogging_enabled", True)
KSIEGI_FILE = settings.get("ksiegi_file", "dupa.txt")
RETRY_COUNT = settings["retry_count"]
RESTART_DELAY = settings["restart_delay"]

write_lock = asyncio.Lock()

def write_data(filename, data):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(data)

async def write_to_file(filename, data):
    async with write_lock:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, write_data, filename, data)

def load_proxies():
    if PROXY_ENABLED and os.path.exists(PROXY_LIST_FILE):
        proxies = []
        with open(PROXY_LIST_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        ip, port, username, password = line.split(":")
                        proxies.append({
                            "server": f"{PROXY_PROTOCOL}://{ip}:{port}",
                            "username": username,
                            "password": password
                        })
                    except ValueError:
                        logger.error(f"Nieprawidłowy format proxy: {line}")
        return proxies
    return []

PROXIES = load_proxies()

def load_ksiegi():
    with open(KSIEGI_FILE, "r", encoding="utf-8") as f:
        return [line.strip().split() for line in f.readlines()]

def load_processed():
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        return {line.strip().split(" - ")[0] for line in f.readlines()}

async def get_public_ip(page):
    try:
        url = "http://httpbin.org/ip"
        await page.goto(url, timeout=2000)
        response = await page.inner_text("body")
        data = json.loads(response)
        return data.get("origin", "Nie udało się pobrać IP")
    except Exception as e:
        return f"Nie udało się pobrać IP: {str(e)}"

def clean_text(text):
    return " ".join(text.split()) if text else "Brak danych"

async def process_ksiega(queue, processed):
    async with async_playwright() as p:
        logger.info("Uruchamianie przeglądarki...")
        browser = await p[BROWSER_TYPE].launch(headless=HEADLESS, args=BROWSER_ARGS)
        try:
            while True:  # Zmieniono na while True, aby nie polegać tylko na queue.empty()
                try:
                    # Sprawdzamy, czy kolejka jest pusta przed pobraniem
                    if queue.empty():
                        logger.info("Kolejka pusta, kończenie pracy workera...")
                        break

                    logger.info(f"Pobieranie zadania z kolejki... Rozmiar kolejki: {queue.qsize()}")
                    kod_wydzialu, numer_ksiegi, cyfra_kontrolna = await queue.get()
                    ksiega_id = f"{kod_wydzialu} {numer_ksiegi}/{cyfra_kontrolna}"
                    logger.info(f"Pobrano zadanie: {ksiega_id}")

                    if ksiega_id in processed:
                        if LOGGING_ENABLED:
                            logger.info(f"⏭️ Pominięto: {ksiega_id} (już przetworzona)")
                        queue.task_done()
                        continue

                    for attempt in range(RETRY_COUNT):
                        proxy = random.choice(PROXIES) if PROXIES else None
                        context = await browser.new_context(
                            user_agent=random.choice(USER_AGENTS) if USER_AGENTS else None,
                            locale=random.choice(LOCALES) if LOCALES else None,
                            ignore_https_errors=False,
                            proxy=proxy
                        )
                        page = await context.new_page()
                        if IP_VERIFICATION_ENABLED:
                            try:
                                public_ip = await asyncio.wait_for(get_public_ip(page), timeout=5)
                                if LOGGING_PROXY_ENABLED and proxy:
                                    logger.info(f"🌐 Przetwarzanie: {ksiega_id} | Proxy: {proxy['server']} | IP: {public_ip}")
                                elif LOGGING_PROXY_ENABLED:
                                    logger.info(f"🌐 Przetwarzanie: {ksiega_id} | Bez proxy | IP: {public_ip}")
                            except asyncio.TimeoutError:
                                logger.warning(f"Timeout podczas weryfikacji IP dla proxy {proxy['server'] if proxy else 'bez proxy'}")
                                await context.close()
                                continue
                            except Exception as e:
                                logger.warning(f"Błąd podczas pobierania IP: {str(e)}")
                                await context.close()
                                continue
                        else:
                            if LOGGING_PROXY_ENABLED and proxy:
                                logger.info(f"🌐 Przetwarzanie: {ksiega_id} | Proxy: {proxy['server']} | Weryfikacja IP wyłączona")
                            elif LOGGING_PROXY_ENABLED:
                                logger.info(f"🌐 Przetwarzanie: {ksiega_id} | Bez proxy | Weryfikacja IP wyłączona")

                        try:
                            logger.info(f"Próba otwarcia strony dla {ksiega_id}...")
                            await page.goto("https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/wyszukiwanieKW", timeout=60000)
                            logger.info(f"Wypełnianie formularza dla {ksiega_id}...")
                            await page.fill("input#kodWydzialuInput", kod_wydzialu)
                            await page.fill("input#numerKsiegiWieczystej", numer_ksiegi)
                            await page.fill("input#cyfraKontrolna", cyfra_kontrolna)
                            await page.click("button#wyszukaj")
                            logger.info(f"Czekanie na załadowanie strony dla {ksiega_id}...")
                            await page.wait_for_load_state("networkidle", timeout=30000)

                            if await page.locator("div.form-row p:has-text('Księga o numerze:')").count():
                                await write_to_file(OUTPUT_FILE, f"{ksiega_id} - NIE ZNALEZIONO\n")
                                if LOGGING_ENABLED:
                                    logger.info(f"❌ Nie znaleziono księgi: {ksiega_id}")
                                break

                            logger.info(f"Pobieranie danych dla {ksiega_id}...")
                            wlas = await page.query_selector_all('div.form-row:has(label:has-text("Właściciel")) p')
                            polozenie = await page.query_selector_all('div.form-row:has(label:has-text("Położenie")) p')
                            zapisanie = await page.query_selector_all('div.form-row:has(label:has-text("Data zapisania księgi wieczystej"))')

                            wlas_data = "; ".join([clean_text(await element.text_content()) for element in wlas]) if wlas else "Brak danych"
                            polozenie_data = "; ".join([clean_text(await element.text_content()) for element in polozenie]) if polozenie else "Brak danych"
                            zapisanie_data = "; ".join([clean_text(await element.text_content()) for element in zapisanie]) if zapisanie else "Brak danych"

                            logger.info(f"Przechodzenie do Dział I-O dla {ksiega_id}...")
                            await page.click("button#przyciskWydrukZupelny")
                            await page.wait_for_load_state("load", timeout=30000)
                            await page.click("text=Dział I-O")
                            await page.wait_for_load_state("load", timeout=30000)

                            logger.info(f"Pobieranie numerów działek dla {ksiega_id}...")
                            parcel_links = await page.query_selector_all('a[href*="mapy.geoportal.gov.pl/imap/?identifyParcel="]')
                            parcel_numbers = [await link.get_attribute("href") for link in parcel_links]
                            parcel_ids = [link.split("identifyParcel=")[-1] for link in parcel_numbers] if parcel_numbers else ["Brak numerów działek"]
                            parcel_ids_str = "; ".join(parcel_ids)

                            if any(data != "Brak danych" for data in [polozenie_data, wlas_data, zapisanie_data, parcel_ids_str]):
                                output = (
                                    f"{ksiega_id} - księga znaleziona, "
                                    f"Położenie: {polozenie_data}, "
                                    f"Właściciele: {wlas_data}, "
                                    f"Data: {zapisanie_data}, "
                                    f"Numery działek (Dział I-O): {parcel_ids_str}"
                                )
                                await write_to_file(OUTPUT_FILE, output + "\n")
                                if LOGGING_ENABLED:
                                    logger.info(f"✅ Zapisano dane dla księgi: {ksiega_id}")
                                    logger.info(f"Sprawdzanie pliku {OUTPUT_FILE}: {os.path.exists(OUTPUT_FILE)}")
                            else:
                                if LOGGING_ENABLED:
                                    logger.warning(f"⚠️ Pominięto zapis dla księgi {ksiega_id} – brak danych.")
                            break
                        except Exception as e:
                            logger.error(f"Błąd dla {ksiega_id}: {str(e)}")
                            html_content = await page.content()
                            logger.info(f"Zawartość strony przy błędzie dla {ksiega_id}: {html_content[:500]}...")
                            await write_to_file(ERROR_LOG, f"{ksiega_id} - ERROR: {str(e)}\n")
                            if PROXY_ERRORLOGGING_ENABLED and proxy:
                                await write_to_file(PROXY_ERROR_LOG, f"{ksiega_id} - Proxy: {proxy['server']} - ERROR: {str(e)}\n")
                            await context.close()
                            if "ERR_TUNNEL_CONNECTION_FAILED" in str(e) or "net::" in str(e):
                                logger.info(f"Próba {attempt + 1} nieudana, zmiana proxy...")
                                continue
                            break
                        finally:
                            await page.close()
                            await context.close()
                except Exception as e:
                    logger.error(f"Nieoczekiwany błąd w pobieraniu zadania: {str(e)}")
                finally:
                    queue.task_done()
                    logger.info(f"Zakończono przetwarzanie zadania dla {ksiega_id}")
        finally:
            await browser.close()
            logger.info("Zamknięto przeglądarkę")

async def main():
    ksiegi = load_ksiegi()
    processed = load_processed()
    queue = asyncio.Queue()

    logger.info(f"Wczytano {len(ksiegi)} ksiąg do przetworzenia")
    for ksiega in ksiegi:
        ksiega_id = f"{ksiega[0]} {ksiega[1]}/{ksiega[2]}"
        if ksiega_id not in processed:
            await queue.put(ksiega)
            logger.info(f"Dodano do kolejki: {ksiega_id}")

    if queue.empty():
        logger.info("✅ Wszystkie księgi przetworzone!")
        return

    logger.info(f"Rozpoczynanie przetwarzania {queue.qsize()} ksiąg...")
    tasks = [asyncio.create_task(process_ksiega(queue, processed)) for _ in range(WORKERS)]
    await queue.join()
    for task in tasks:
        task.cancel()
    logger.info("Zakończono wszystkie zadania")

if __name__ == "__main__":
    asyncio.run(main())