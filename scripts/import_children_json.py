# File: import_children_json.py

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.config import DB_PATH


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_TABLEAU_DIR = PROJECT_ROOT / "data" / "raw_tableau"

TARGET_PREFIX = "children_bookings_2025-04-"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_json_files() -> list[Path]:
    return sorted(RAW_TABLEAU_DIR.glob(f"{TARGET_PREFIX}*.json"))


def import_children_json() -> int:
    json_files = list_json_files()

    if not json_files:
        print("Nessun JSON trovato per aprile 2025.")
        return 0

    inserted = 0

    with get_connection() as conn:
        conn.execute(
            """
            DELETE FROM tableau_reservations
            WHERE source_file LIKE 'children_bookings_2025-04-%'
            """
        )

        for file_path in json_files:
            with file_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            source_day = payload.get("day", "")
            bookings = payload.get("bookings", [])

            for booking in bookings:
                conn.execute(
                    """
                    INSERT INTO tableau_reservations (
                        reservation_id,
                        unit_name,
                        guest_name,
                        check_in,
                        check_out,
                        nights,
                        adults,
                        children,
                        source_day,
                        source_file
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        booking.get("reservation_id", ""),
                        "",
                        booking.get("guest_name", ""),
                        booking.get("check_in", ""),
                        booking.get("check_out", ""),
                        int(booking.get("nights", 0) or 0),
                        int(booking.get("adults_count", 0) or 0),
                        int(booking.get("children_count", 0) or 0),
                        source_day,
                        file_path.name,
                    ),
                )
                inserted += 1

        conn.commit()

    return inserted


def main() -> None:
    inserted = import_children_json()
    print(f"Prenotazioni importate in tableau_reservations: {inserted}")


if __name__ == "__main__":
    main()

# EOF - import_children_json.py