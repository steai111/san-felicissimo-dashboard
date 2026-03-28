# File: dashboard_update_orchestrator.py

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from app.beddy_session import open_beddy_session, close_beddy_session
from app.config import DB_PATH

import scripts.extract_children_bookings as extract_children_bookings_module
import scripts.extract_unit_nights as extract_unit_nights_module


BEDDY_STATS_URL = "https://app.beddy.io/stats/revenue"

# Periodo di lavoro: modificabile manualmente
START_DATE = date(2026, 3, 13)
END_DATE = date(2026, 3, 28)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CSV_DIR = PROJECT_ROOT / "data" / "raw_csv"
RAW_GA4_DIR = PROJECT_ROOT / "data" / "raw_ga4"
RAW_TABLEAU_DIR = PROJECT_ROOT / "data" / "raw_tableau"
RAW_TABLEAU_UNITS_DIR = PROJECT_ROOT / "data" / "raw_tableau_units"


def print_step(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def wait_for_enter(message: str) -> None:
    input(f"{message}\nPremi INVIO per continuare... ")


def run_module(module_name: str) -> None:
    print(f"\n[RUN] python3 -m {module_name}")
    result = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=str(PROJECT_ROOT),
    )

    if result.returncode != 0:
        raise RuntimeError(f"Errore durante esecuzione modulo: {module_name}")

    print(f"[OK] Modulo completato: {module_name}")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def iter_days(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def count_csv_files() -> tuple[int, int]:
    csv_files = list(RAW_CSV_DIR.glob("*.csv"))

    channel_count = 0
    nationality_count = 0

    for file_path in csv_files:
        name = file_path.name.lower()
        if "canal" in name or "prenot" in name:
            channel_count += 1
        if "nazion" in name:
            nationality_count += 1

    return channel_count, nationality_count


def wait_for_csv_export() -> None:
    print_step("STEP 2 — Export CSV da Beddy")

    wait_for_enter(
        "Esporta i 2 CSV del periodo scelto da Beddy "
        "(canali prenotazione + nazionalità), "
        "copiali nella cartella data/raw_csv e poi torna qui."
    )

    channel_count, nationality_count = count_csv_files()

    print(f"\nCSV canali trovati: {channel_count}")
    print(f"CSV nazionalità trovati: {nationality_count}")

    if channel_count == 0 or nationality_count == 0:
        raise RuntimeError(
            "CSV mancanti in data/raw_csv. "
            "Servono almeno 1 CSV canali e 1 CSV nazionalità."
        )

    print("[OK] CSV presenti, possiamo procedere.")


def run_csv_import() -> None:
    print_step("STEP 3 — Import CSV nel database")
    run_module("scripts.import_beddy_csv")


def wait_for_ga4_file() -> Path:
    print_step("STEP 4 — Inserimento file GA4")

    wait_for_enter(
        "Crea o copia il file data/raw_ga4/ga4_current.json "
        "con il dato delle sessioni GA4 del periodo scelto, poi torna qui."
    )

    file_path = RAW_GA4_DIR / "ga4_current.json"
    if not file_path.exists():
        raise RuntimeError("File GA4 mancante: data/raw_ga4/ga4_current.json")

    print(f"[OK] File GA4 trovato: {file_path.name}")
    return file_path


def import_ga4_current_json(file_path: Path) -> None:
    print_step("STEP 5 — Import GA4 nel database")

    with file_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    period_start = payload["period_start"]
    period_end = payload["period_end"]
    sessions = int(payload["sessions"])

    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ga4_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value INTEGER NOT NULL,
                source_detail TEXT,
                imported_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            DELETE FROM ga4_metrics
            WHERE period_start = ?
              AND period_end = ?
              AND metric_name = 'sessions'
            """,
            (period_start, period_end),
        )

        conn.execute(
            """
            INSERT INTO ga4_metrics (
                period_start,
                period_end,
                metric_name,
                metric_value,
                source_detail
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                period_start,
                period_end,
                "sessions",
                sessions,
                file_path.name,
            ),
        )

        conn.commit()

    print(f"[OK] GA4 importato: sessions={sessions}")


def clean_children_json_outputs() -> None:
    for file_path in RAW_TABLEAU_DIR.glob("children_bookings_*.json"):
        file_path.unlink()


def clean_unit_nights_json_outputs() -> None:
    for file_path in RAW_TABLEAU_UNITS_DIR.glob("unit_nights_*.json"):
        file_path.unlink()


def run_children_extraction() -> None:
    print_step("STEP 6 — Estrazione Children dal tableau")

    clean_children_json_outputs()

    extract_children_bookings_module.START_DATE = START_DATE
    extract_children_bookings_module.END_DATE = END_DATE
    extract_children_bookings_module.main()

    print("[OK] Estrazione children completata.")


def confirm_children_jsons() -> None:
    wait_for_enter(
        "Controlla i JSON children in data/raw_tableau. "
        "Se sono corretti, torna qui."
    )


def import_children_json_for_period() -> int:
    print_step("STEP 7 — Import Children nel database")

    json_files = []
    for current_day in iter_days(START_DATE, END_DATE):
        file_path = RAW_TABLEAU_DIR / f"children_bookings_{current_day.isoformat()}.json"
        if file_path.exists():
            json_files.append(file_path)

    if not json_files:
        raise RuntimeError("Nessun JSON children trovato per il periodo selezionato.")

    inserted = 0

    with get_connection() as conn:
        for current_day in iter_days(START_DATE, END_DATE):
            conn.execute(
                """
                DELETE FROM tableau_reservations
                WHERE source_day = ?
                """,
                (current_day.isoformat(),),
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

    print(f"[OK] Prenotazioni children importate: {inserted}")
    return inserted


def run_unit_nights_extraction() -> None:
    print_step("STEP 8 — Estrazione Notti per unità dal tableau")

    clean_unit_nights_json_outputs()

    extract_unit_nights_module.START_DATE = START_DATE
    extract_unit_nights_module.END_DATE = END_DATE
    extract_unit_nights_module.main()

    print("[OK] Estrazione unit nights completata.")


def confirm_unit_nights_jsons() -> None:
    wait_for_enter(
        "Controlla i JSON unit nights in data/raw_tableau_units. "
        "Se sono corretti, torna qui."
    )


def import_unit_nights_for_period() -> int:
    print_step("STEP 9 — Import Notti per unità nel database")

    json_files = []
    for current_day in iter_days(START_DATE, END_DATE):
        file_path = RAW_TABLEAU_UNITS_DIR / f"unit_nights_{current_day.isoformat()}.json"
        if file_path.exists():
            json_files.append(file_path)

    if not json_files:
        raise RuntimeError("Nessun JSON unit nights trovato per il periodo selezionato.")

    inserted = 0

    with get_connection() as conn:
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

        for current_day in iter_days(START_DATE, END_DATE):
            conn.execute(
                """
                DELETE FROM unit_nights_daily
                WHERE day = ?
                """,
                (current_day.isoformat(),),
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

    print(f"[OK] Righe unit nights importate: {inserted}")
    return inserted


def print_final_report() -> None:
    print_step("STEP 10 — Report finale database")

    with get_connection() as conn:
        cursor = conn.cursor()

        period_start = START_DATE.isoformat()
        period_end = END_DATE.isoformat()
        month_key = START_DATE.strftime("%Y-%m")

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM beddy_channel_stats
            WHERE substr(period_start, 1, 7) = ?
            """,
            (month_key,),
        )
        channel_rows = cursor.fetchone()["total"]

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM beddy_nationality_stats
            WHERE substr(period_start, 1, 7) = ?
            """,
            (month_key,),
        )
        nationality_rows = cursor.fetchone()["total"]

        cursor.execute(
            """
            SELECT metric_value
            FROM ga4_metrics
            WHERE period_start = ? AND period_end = ?
              AND metric_name = 'sessions'
            LIMIT 1
            """,
            (period_start, period_end),
        )
        ga4_row = cursor.fetchone()
        ga4_sessions = ga4_row["metric_value"] if ga4_row else 0

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM tableau_reservations
            WHERE source_day >= ? AND source_day <= ?
            """,
            (period_start, period_end),
        )
        children_rows = cursor.fetchone()["total"]

        cursor.execute(
            """
            SELECT COUNT(*) AS total
            FROM unit_nights_daily
            WHERE day >= ? AND day <= ?
            """,
            (period_start, period_end),
        )
        unit_rows = cursor.fetchone()["total"]

    print(f"Periodo elaborato: {period_start} -> {period_end}")
    print(f"Righe canali: {channel_rows}")
    print(f"Righe nazionalità: {nationality_rows}")
    print(f"GA4 sessions: {ga4_sessions}")
    print(f"Prenotazioni tableau_reservations: {children_rows}")
    print(f"Righe unit_nights_daily: {unit_rows}")


def main() -> None:
    print_step("DASHBOARD UPDATE ORCHESTRATOR")
    print(f"Periodo configurato: {START_DATE.isoformat()} -> {END_DATE.isoformat()}")

    print_step("STEP 1 — Apertura Beddy su Statistiche")
    page = open_beddy_session(BEDDY_STATS_URL)
    print(f"Pagina aperta: {page.url}")

    wait_for_enter(
        "Completa il login su Beddy, verifica di essere dentro alla pagina statistiche,"
        " poi torna qui."
    )

    print("\nBrowser ancora aperto e sessione attiva.")

    wait_for_csv_export()
    run_csv_import()

    ga4_file = wait_for_ga4_file()
    import_ga4_current_json(ga4_file)

    wait_for_enter(
        "Premi INVIO per chiudere il browser Beddy statistiche e passare agli agenti tableau."
    )
    close_beddy_session(page)

    run_children_extraction()
    confirm_children_jsons()
    import_children_json_for_period()

    run_unit_nights_extraction()
    confirm_unit_nights_jsons()
    import_unit_nights_for_period()

    print_final_report()

    wait_for_enter("Pipeline completata. Premi INVIO per terminare.")


if __name__ == "__main__":
    main()

# EOF - dashboard_update_orchestrator.py