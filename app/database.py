import sqlite3
from pathlib import Path

from app.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Restituisce una connessione SQLite al database del progetto."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Crea le tabelle base del progetto se non esistono."""
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_key TEXT NOT NULL,
                metric_label TEXT NOT NULL,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                metric_value TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_detail TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS beddy_channel_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                total_bookings INTEGER,
                cancelled_bookings INTEGER,
                nights_sold REAL,
                average_stay REAL,
                arrivals INTEGER,
                departures INTEGER,
                revenue REAL,
                occupancy_rate REAL,
                incidence_rate REAL,
                source_file TEXT,
                imported_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS beddy_nationality_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_start TEXT NOT NULL,
                period_end TEXT NOT NULL,
                nationality TEXT NOT NULL,
                nights_sold REAL,
                total_bookings INTEGER,
                average_stay REAL,
                arrivals INTEGER,
                departures INTEGER,
                source_file TEXT,
                imported_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tableau_reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reservation_id TEXT,
                unit_name TEXT NOT NULL,
                guest_name TEXT,
                check_in TEXT,
                check_out TEXT,
                nights INTEGER,
                adults INTEGER,
                children INTEGER,
                source_day TEXT,
                source_file TEXT,
                imported_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
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

        cursor.execute(
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

        conn.commit()


if __name__ == "__main__":
    init_db()
    print(f"Database inizializzato correttamente: {Path(DB_PATH)}")