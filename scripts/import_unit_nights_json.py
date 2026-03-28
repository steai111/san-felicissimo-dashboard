# File: import_unit_nights_json.py

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.config import DB_PATH


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_UNITS_DIR = PROJECT_ROOT / "data" / "raw_tableau_units"

TARGET_PREFIX = "unit_nights_2025-04-"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_json_files() -> list[Path]:
    return sorted(RAW_UNITS_DIR.glob(f"{TARGET_PREFIX}*.json"))


def ensure_unit_nights_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS unit_nights_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT NOT NULL,
            unit_code TEXT NOT NULL,
            unit_label TEXT NOT NULL,
            is_sold INTEGER NOT NULL,
            is_free INTEGER NOT NULL,
            source_file TEXT,
            imported_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def import_unit_nights_json() -> int:
    json_files = list_json_files()

    if not json_files:
        print("Nessun JSON unit_nights trovato per aprile 2025.")
        return 0

    inserted = 0

    with get_connection() as conn:
        ensure_unit_nights_table(conn)

        conn.execute(
            """
            DELETE FROM unit_nights_daily
            WHERE source_file LIKE 'unit_nights_2025-04-%'
            """
        )

        for file_path in json_files:
            with file_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            day = payload.get("day", "")
            units = payload.get("units", [])

            for unit in units:
                conn.execute(
                    """
                    INSERT INTO unit_nights_daily (
                        day,
                        unit_code,
                        unit_label,
                        is_sold,
                        is_free,
                        source_file
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        day,
                        unit.get("unit_code", ""),
                        unit.get("unit_label", ""),
                        1 if unit.get("is_sold", False) else 0,
                        1 if unit.get("is_free", False) else 0,
                        file_path.name,
                    ),
                )
                inserted += 1

        conn.commit()

    return inserted


def main() -> None:
    inserted = import_unit_nights_json()
    print(f"Righe importate in unit_nights_daily: {inserted}")


if __name__ == "__main__":
    main()

# EOF - import_unit_nights_json.py