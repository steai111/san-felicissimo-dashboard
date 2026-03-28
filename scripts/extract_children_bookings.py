# File: extract_children_bookings.py

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from app.beddy_session import open_beddy_session, close_beddy_session
from app.beddy_popup_extractor import extract_popup_booking_data


TABLEAU_BASE_URL = "https://app.beddy.io/tableau"

# Periodo di analisi: modificabile manualmente
START_DATE = date(2026, 3, 28)
END_DATE = date(2026, 3, 29)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw_tableau"


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def iter_days(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_tableau_url(day: date) -> str:
    return f"{TABLEAU_BASE_URL}?start={day.isoformat()}"


def save_day_json(day: date, bookings: list[dict]) -> Path:
    output_path = OUTPUT_DIR / f"children_bookings_{day.isoformat()}.json"

    payload = {
        "source": "beddy_tableau_popup",
        "day": day.isoformat(),
        "bookings": bookings,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return output_path


def get_visible_booking_boxes(page, target_day: int) -> list[tuple[int, str, dict]]:
    """
    Restituisce solo i box che PARTONO nel giorno target.
    Lavora verticalmente sulla colonna del giorno, usando il bordo sinistro del box
    con una tolleranza stretta.
    """
    page.wait_for_timeout(3000)
    page.wait_for_selector("div.by-tableau-reservation.by-tableau-box", timeout=10000)

    day_headers = page.locator("th.by-tableau-cell--day")
    day_headers_count = day_headers.count()

    target_box = None

    for i in range(day_headers_count):
        header = day_headers.nth(i)
        header_text = " ".join(header.inner_text().split())

        if header_text.startswith(f"{target_day} "):
            target_box = header.bounding_box()
            print(f"COLONNA TARGET TROVATA: index={i} text='{header_text}'")
            break

    if not target_box:
        print(f"ERRORE: nessuna colonna trovata per il giorno {target_day}")
        return []

    column_start_x = target_box["x"]
    column_width = target_box["width"]

    start_tolerance_left = 10
    start_tolerance_right = max(10, column_width * 0.35)

    min_start_x = column_start_x - start_tolerance_left
    max_start_x = column_start_x + start_tolerance_right

    print(f"RANGE START X GIORNO {target_day}: {min_start_x} -> {max_start_x}")

    reservation_boxes = page.locator("div.by-tableau-reservation.by-tableau-box")
    boxes_count = reservation_boxes.count()
    print(f"BOX PRENOTAZIONE VISIBILI: {boxes_count}")

    starting_boxes = []

    for i in range(boxes_count):
        box_locator = reservation_boxes.nth(i)
        box_rect = box_locator.bounding_box()

        if not box_rect:
            continue

        try:
            box_name = box_locator.locator("div.by-tableau-reservation__content").inner_text().strip()
        except Exception:
            box_name = f"box_{i+1}"

        box_start_x = box_rect["x"]

        if min_start_x <= box_start_x <= max_start_x:
            starting_boxes.append((i, box_name, box_rect))
            print(f"BOX START DAY {target_day}: index={i + 1} name='{box_name}' rect={box_rect}")

    print(f"TOTALE BOX CHE INIZIANO IL GIORNO {target_day}: {len(starting_boxes)}")
    return starting_boxes


def expand_popup_details(page) -> None:
    """
    Clicca la riga header del popup per espandere i dettagli.
    Non clicchiamo l'icona <i>, ma il contenitore cliccabile padre.
    """
    details_row = page.locator(
        "nz-modal-container by-tableau-modal-header-details-row .by-tableau-modal-header-details-row"
    ).last

    details_row.wait_for(timeout=5000)
    details_row.click(force=True)
    page.wait_for_timeout(800)


def close_popup(page) -> None:
    """
    Chiude il popup prenotazione.
    """
    close_button = page.locator("nz-modal-container button[aria-label='Close'], nz-modal-container .ant-modal-close").last
    try:
        close_button.wait_for(timeout=3000)
        close_button.click()
    except Exception:
        page.keyboard.press("Escape")
    page.wait_for_timeout(700)


def scan_single_day(page, day: date, max_items: int = 3) -> list[dict]:
    """
    Primo test reale:
    - apre il tableau sul giorno richiesto
    - individua solo i box che iniziano in quel giorno
    - apre i primi N
    - estrae i dati dal popup
    """
    page.goto(build_tableau_url(day), wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)

    boxes = get_visible_booking_boxes(page, target_day=day.day)

    extracted = []
    seen_reservation_ids = set()

    for idx, (_, box_name, box_rect) in enumerate(boxes[:max_items], start=1):
        try:     

            print(f"\n[{idx}] Apro box: {box_name}")

            click_x = box_rect["x"] + (box_rect["width"] / 2)
            click_y = box_rect["y"] + (box_rect["height"] / 2)

            page.mouse.click(click_x, click_y)
            page.wait_for_timeout(1000)

            expand_popup_details(page)
            data = extract_popup_booking_data(page)
            reservation_id = data.get("reservation_id", "").strip()

            if reservation_id and reservation_id in seen_reservation_ids:
                print(f"    SALTATO -> reservation_id duplicato {reservation_id}")
                close_popup(page)
                continue

            if reservation_id:
                seen_reservation_ids.add(reservation_id)

            expected_check_in = day.strftime("%d/%m/%Y")

            if data["check_in"] != expected_check_in:
                print(
                    f"    SALTATO -> check-in reale {data['check_in']} "
                    f"diverso da target {expected_check_in}"
                )
                close_popup(page)
                continue

            data["source_day"] = day.isoformat()
            data["box_text"] = box_name

            extracted.append(data)

            print(
                f"    OK -> reservation_id={data['reservation_id']} | "
                f"guest_name={data['guest_name']} | "
                f"adults={data['adults_count']} | "
                f"children={data['children_count']}"
            )

            close_popup(page)

        except Exception as exc:
            print(f"    ERRORE su box {idx}: {exc}")
            try:
                close_popup(page)
            except Exception:
                pass

    return extracted


def main() -> None:
    ensure_output_dir()

    page = open_beddy_session(build_tableau_url(START_DATE))

    print("\nSessione pronta.")
    print(f"Periodo configurato: {START_DATE.isoformat()} -> {END_DATE.isoformat()}")
    print("Test attuale: scansione reale dell'intero periodo configurato.")

    total_extracted = 0

    for current_day in iter_days(START_DATE, END_DATE):
        print(f"\n===== SCANSIONE GIORNO {current_day.isoformat()} =====")
        bookings = scan_single_day(page, current_day, max_items=50)
        output_path = save_day_json(current_day, bookings)

        print(f"[OK] File JSON creato: {output_path.name}")
        print(f"Prenotazioni estratte nel giorno: {len(bookings)}")

        total_extracted += len(bookings)

    print(f"\nTotale prenotazioni estratte nel periodo: {total_extracted}")

    input("\nPremi INVIO per chiudere il browser... ")
    close_beddy_session(page)


if __name__ == "__main__":
    main()

# EOF - extract_children_bookings.py