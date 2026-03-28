from __future__ import annotations

import re
from playwright.sync_api import Page


def _extract_int(pattern: str, text: str) -> int:
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return 0
    return int(match.group(1))


def _extract_text(pattern: str, text: str) -> str:
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def extract_popup_booking_data(page: Page) -> dict:
    """
    Estrae i dati minimi dal popup prenotazione del tableau Beddy.
    Richiede che il popup sia già aperto e che la freccia dettagli sia già espansa.
    """
    popup = page.locator("nz-modal-container").last
    popup.wait_for(timeout=10000)

    popup_text = popup.inner_text()

    guest_name = _extract_text(r"^\s*(.+?)\n", popup_text)

    reservation_id = _extract_text(r"ID:\s*([A-Z0-9]+)", popup_text)

    check_in = _extract_text(r"(\d{1,2}/\d{1,2}/\d{4})", popup_text)
    check_out = ""
    date_matches = re.findall(r"(\d{1,2}/\d{1,2}/\d{4})", popup_text)
    if len(date_matches) >= 2:
        check_in = date_matches[0]
        check_out = date_matches[1]

    nights = _extract_int(r"(\d+)\s+Nott", popup_text)
    adults_count = _extract_int(r"(\d+)\s+Adult", popup_text)
    children_count = _extract_int(r"(\d+)\s+Bambin", popup_text)

    return {
        "reservation_id": reservation_id,
        "guest_name": guest_name,
        "check_in": check_in,
        "check_out": check_out,
        "nights": nights,
        "adults_count": adults_count,
        "children_count": children_count,
    }


# EOF - beddy_popup_extractor.py