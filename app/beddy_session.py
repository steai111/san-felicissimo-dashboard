# beddy_session.py
# Gestione sessione browser Playwright per Data Dashboard
# Login con OTP manuale inserito dall'utente nel browser

from __future__ import annotations

import time
from pathlib import Path
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, Playwright

from app.beddy_credentials import BEDDY_USERNAME, BEDDY_PASSWORD


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SESSION_DIR = PROJECT_ROOT / "data" / "session"
SESSION_FILE = SESSION_DIR / "beddy_session.json"

BEDDY_LOGIN_URL = "https://app.beddy.io/login"


def ensure_session_folder() -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True)


def open_beddy_session(target_url: str) -> Page:
    """
    Apre una sessione Beddy riutilizzando storage_state se presente.
    Se la sessione non è valida, esegue login con OTP manuale:
    l'utente inserisce il codice nel browser, poi conferma da terminale.
    Alla fine atterra sulla target_url richiesta e restituisce la page viva.
    """
    ensure_session_folder()

    playwright: Playwright = sync_playwright().start()
    browser: Browser = playwright.chromium.launch(headless=False)

    if SESSION_FILE.exists():
        context: BrowserContext = browser.new_context(storage_state=str(SESSION_FILE))
        print("Sessione Beddy caricata correttamente.")
    else:
        context = browser.new_context()
        print("Nessuna sessione trovata. Browser aperto senza sessione salvata.")

    page: Page = context.new_page()

    page.goto(target_url, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")

    if "login" in page.url.lower():
        print("Sessione non valida o scaduta. Avvio login con OTP manuale.")

        # Username
        username_input = page.locator('input[formcontrolname="username"]')
        username_input.wait_for(timeout=10000)
        username_input.fill(BEDDY_USERNAME)

        # Password
        password_input = page.locator('input[formcontrolname="password"]')
        password_input.wait_for(timeout=10000)
        password_input.fill(BEDDY_PASSWORD)

        # Primo Accedi
        login_button = page.locator('button[type="submit"]')
        login_button.wait_for(timeout=10000)
        login_button.click()
        page.wait_for_load_state("networkidle")

        # Selettore metodo OTP
        otp_selector = page.locator("nz-select-item")
        otp_selector.wait_for(timeout=10000)
        otp_selector.click()

        # Metodo email
        page.get_by_text("Autenticazione con email", exact=False).click()

        # Avanti
        page.get_by_text("Avanti", exact=False).click()
        page.wait_for_load_state("networkidle")

        print("\nInserisci manualmente il codice OTP nel browser.")
        input("Quando hai completato il login in Beddy, premi INVIO qui nel terminale... ")

        # Attesa consolidamento login
        login_completed = False
        for _ in range(20):
            page.wait_for_load_state("networkidle")
            current_url = page.url
            print(f"URL durante verifica login: {current_url}")

            if "login" not in current_url.lower():
                login_completed = True
                break

            time.sleep(1)

        if not login_completed:
            raise Exception(f"Login non completato: URL finale ancora su login -> {page.url}")

        # Salva sessione
        context.storage_state(path=str(SESSION_FILE))
        print("Login completato e sessione aggiornata.")

        # Vai alla pagina target vera
        page.goto(target_url, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")

    print(f"Pagina pronta. URL corrente: {page.url}")

    # Manteniamo vivi browser/context/playwright attaccandoli alla page
    page._beddy_browser = browser
    page._beddy_context = context
    page._beddy_playwright = playwright

    return page


def close_beddy_session(page: Page) -> None:
    """Chiude correttamente browser e Playwright."""
    page._beddy_browser.close()
    page._beddy_playwright.stop()


if __name__ == "__main__":
    test_url = "https://app.beddy.io/tableau"
    page = open_beddy_session(test_url)
    input("Premi INVIO per chiudere il browser... ")
    close_beddy_session(page)

# EOF - beddy_session.py