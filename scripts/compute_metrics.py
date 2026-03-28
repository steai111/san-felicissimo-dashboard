import sqlite3

from app.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Apre una connessione al database SQLite del progetto."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def print_section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main() -> None:
    with get_connection() as conn:
        cursor = conn.cursor()

        print_section("RIEPILOGO TABELLE")
        cursor.execute("SELECT COUNT(*) AS total FROM beddy_channel_stats")
        channel_rows = cursor.fetchone()["total"]

        cursor.execute("SELECT COUNT(*) AS total FROM beddy_nationality_stats")
        nationality_rows = cursor.fetchone()["total"]

        cursor.execute("SELECT COUNT(*) AS total FROM tableau_reservations")
        tableau_rows = cursor.fetchone()["total"]

        print(f"beddy_channel_stats: {channel_rows}")
        print(f"beddy_nationality_stats: {nationality_rows}")
        print(f"tableau_reservations: {tableau_rows}")

        print_section("DETTAGLIO CANALI")
        cursor.execute(
            """
            SELECT
                period_start,
                period_end,
                channel_name,
                total_bookings,
                average_stay,
                nights_sold
            FROM beddy_channel_stats
            ORDER BY total_bookings DESC, channel_name ASC
            """
        )
        for row in cursor.fetchall():
            print(
                f"{row['period_start']} -> {row['period_end']} | "
                f"{row['channel_name']} | "
                f"prenotazioni={row['total_bookings']} | "
                f"sogg_medio={row['average_stay']} | "
                f"notti={row['nights_sold']}"
            )

        print_section("DETTAGLIO NAZIONALITÀ")
        cursor.execute(
            """
            SELECT
                period_start,
                period_end,
                nationality,
                nights_sold,
                total_bookings,
                average_stay
            FROM beddy_nationality_stats
            ORDER BY nights_sold DESC, nationality ASC
            """
        )
        for row in cursor.fetchall():
            print(
                f"{row['period_start']} -> {row['period_end']} | "
                f"{row['nationality']} | "
                f"presenze/notti={row['nights_sold']} | "
                f"prenotazioni={row['total_bookings']} | "
                f"sogg_medio={row['average_stay']}"
            )

        print_section("PRIME 5 RIGHE GREZZE BEDDY_CHANNEL_STATS")
        cursor.execute(
            """
            SELECT id, channel_name, total_bookings, average_stay, nights_sold
            FROM beddy_channel_stats
            ORDER BY id ASC
            LIMIT 5
            """
        )
        for row in cursor.fetchall():
            print(
                f"id={row['id']} | "
                f"channel_name={row['channel_name']} | "
                f"total_bookings={row['total_bookings']} | "
                f"average_stay={row['average_stay']} | "
                f"nights_sold={row['nights_sold']}"
            )


if __name__ == "__main__":
    main()