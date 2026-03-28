# File: import_ga4_sessions.py

from __future__ import annotations

import sqlite3

from app.config import DB_PATH


PERIOD_START = "2025-04-01"
PERIOD_END = "2025-04-30"
SESSIONS_VALUE = 968


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_ga4_table(conn: sqlite3.Connection) -> None:
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


def main() -> None:
    with get_connection() as conn:
        ensure_ga4_table(conn)

        conn.execute(
            """
            DELETE FROM ga4_metrics
            WHERE period_start = ?
              AND period_end = ?
              AND metric_name = 'sessions'
            """,
            (PERIOD_START, PERIOD_END),
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
                PERIOD_START,
                PERIOD_END,
                "sessions",
                SESSIONS_VALUE,
                "GA4 session_start / aprile 2025",
            ),
        )

        conn.commit()

    print(f"GA4 importato correttamente: sessions={SESSIONS_VALUE}")


if __name__ == "__main__":
    main()

# EOF - import_ga4_sessions.py