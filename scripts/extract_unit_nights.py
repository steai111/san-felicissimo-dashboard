# File: extract_unit_nights.py

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from app.beddy_session import open_beddy_session, close_beddy_session


TABLEAU_BASE_URL = "https://app.beddy.io/tableau"

# Periodo di analisi: modificabile manualmente
START_DATE = date(2025, 4, 17)
END_DATE = date(2025, 4, 30)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw_tableau_units"


UNIT_MAPPING = {
    "1": "Camera 1",
    "2": "Camera 2",
    "3": "Camera 3",
    "4": "Camera 4",
    "5": "Appartamento Vite",
    "6": "Appartamento Ulivo",
    "7": "Appartamento Cipresso",
}

EXCLUDED_UNIT_CODES = {"9"}


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def iter_days(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_tableau_url(day: date) -> str:
    return f"{TABLEAU_BASE_URL}?start={day.isoformat()}"


def save_day_json(day: date, units: list[dict]) -> Path:
    output_path = OUTPUT_DIR / f"unit_nights_{day.isoformat()}.json"

    payload = {
        "source": "beddy_tableau_units",
        "day": day.isoformat(),
        "units": units,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return output_path


def get_target_day_range(page, target_day: int) -> tuple[float, float] | None:
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
        return None

    column_start_x = target_box["x"]
    column_end_x = target_box["x"] + target_box["width"]

    print(f"RANGE GIORNO {target_day}: {column_start_x} -> {column_end_x}")
    return column_start_x, column_end_x


def parse_unit_rows(page) -> list[dict]:
    """
    Legge le righe unità reali dal tableau usando:
    - una riga gruppo (Camera/Appartamento...)
    - seguita da una riga che inizia con il codice unità
    """
    row_locators = page.locator("tr")
    row_count = row_locators.count()

    parsed_units = []
    current_group_name = ""

    for i in range(row_count):
        row = row_locators.nth(i)

        try:
            row_text = " ".join(row.inner_text().split())
        except Exception:
            continue

        if not row_text:
            continue

        upper_text = row_text.upper()

        if upper_text.startswith("CAMERA MATRIMONIALE CON VISTA PANORAMICA"):
            current_group_name = "Camera Matrimoniale con vista panoramica"
            continue

        if upper_text.startswith("APPARTAMENTO VITE"):
            current_group_name = "Appartamento Vite"
            continue

        if upper_text.startswith("APPARTAMENTO ULIVO"):
            current_group_name = "Appartamento Ulivo"
            continue

        if upper_text.startswith("APPARTAMENTO CIPRESSO"):
            current_group_name = "Appartamento Cipresso"
            continue

        if upper_text.startswith("CAMERE PER SPOSTAMENTI"):
            current_group_name = "CAMERE PER SPOSTAMENTI"
            continue

        parts = row_text.split()
        if not parts:
            continue

        unit_code = parts[0]

        if unit_code in EXCLUDED_UNIT_CODES:
            continue

        if unit_code in UNIT_MAPPING and current_group_name:
            row_box = row.bounding_box()
            if not row_box:
                continue

            parsed_units.append(
                {
                    "unit_code": unit_code,
                    "unit_label": UNIT_MAPPING[unit_code],
                    "group_name": current_group_name,
                    "row_y": row_box["y"],
                    "row_height": row_box["height"],
                }
            )

    print(f"UNITÀ TROVATE NEL TABLEAU: {len(parsed_units)}")
    for unit in parsed_units:
        print(
            f"- {unit['unit_code']} -> {unit['unit_label']} | "
            f"group={unit['group_name']} | y={unit['row_y']}"
        )

    return parsed_units


def get_visible_booking_boxes(page) -> list[dict]:
    reservation_boxes = page.locator("div.by-tableau-reservation.by-tableau-box")
    boxes_count = reservation_boxes.count()
    print(f"BOX PRENOTAZIONE VISIBILI: {boxes_count}")

    visible_boxes = []

    for i in range(boxes_count):
        box_locator = reservation_boxes.nth(i)
        box_rect = box_locator.bounding_box()

        if not box_rect:
            continue

        try:
            box_name = box_locator.locator("div.by-tableau-reservation__content").inner_text().strip()
        except Exception:
            box_name = f"box_{i+1}"

        visible_boxes.append(
            {
                "index": i,
                "name": box_name,
                "x": box_rect["x"],
                "y": box_rect["y"],
                "width": box_rect["width"],
                "height": box_rect["height"],
            }
        )

    return visible_boxes


def row_has_booking_on_day(unit_row: dict, boxes: list[dict], day_start_x: float, day_end_x: float) -> bool:
    """
    Controlla se nella riga dell'unità esiste almeno un box che copre il giorno target.
    """
    row_top = unit_row["row_y"]
    row_bottom = unit_row["row_y"] + unit_row["row_height"]

    for box in boxes:
        box_top = box["y"]
        box_bottom = box["y"] + box["height"]

        vertical_overlap = not (box_bottom < row_top or box_top > row_bottom)
        if not vertical_overlap:
            continue

        box_start_x = box["x"]
        box_end_x = box["x"] + box["width"]

        horizontal_overlap = not (box_end_x <= day_start_x or box_start_x >= day_end_x)
        if not horizontal_overlap:
            continue

        return True

    return False


def scan_single_day_units(page, day: date) -> list[dict]:
    page.goto(build_tableau_url(day), wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)

    day_range = get_target_day_range(page, target_day=day.day)
    if not day_range:
        return []

    day_start_x, day_end_x = day_range
    units = parse_unit_rows(page)
    boxes = get_visible_booking_boxes(page)

    results = []

    for unit in units:
        is_sold = row_has_booking_on_day(unit, boxes, day_start_x, day_end_x)
        results.append(
            {
                "unit_code": unit["unit_code"],
                "unit_label": unit["unit_label"],
                "is_sold": is_sold,
                "is_free": not is_sold,
            }
        )
        print(
            f"UNITÀ {unit['unit_label']} -> "
            f"{'VENDUTA' if is_sold else 'LIBERA'}"
        )

    return results


def main() -> None:
    ensure_output_dir()

    page = open_beddy_session(build_tableau_url(START_DATE))

    print("\nSessione pronta.")
    print(f"Periodo configurato: {START_DATE.isoformat()} -> {END_DATE.isoformat()}")
    print("Test attuale: lettura reale dell'intero periodo configurato per unità.")

    total_days = 0

    for current_day in iter_days(START_DATE, END_DATE):
        print(f"\n===== SCANSIONE GIORNO {current_day.isoformat()} =====")
        units = scan_single_day_units(page, current_day)
        output_path = save_day_json(current_day, units)

        print(f"[OK] File JSON creato: {output_path.name}")
        print(f"Unità elaborate nel giorno: {len(units)}")

        total_days += 1

    print(f"\nTotale giorni elaborati: {total_days}")

    input("\nPremi INVIO per chiudere il browser... ")
    close_beddy_session(page)


if __name__ == "__main__":
    main()

# EOF - extract_unit_nights.py