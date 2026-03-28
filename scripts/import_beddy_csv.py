import calendar
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

from app.config import DB_PATH, RAW_CSV_DIR


ITALIAN_MONTHS = {
    "gennaio": 1,
    "febbraio": 2,
    "marzo": 3,
    "aprile": 4,
    "maggio": 5,
    "giugno": 6,
    "luglio": 7,
    "agosto": 8,
    "settembre": 9,
    "ottobre": 10,
    "novembre": 11,
    "dicembre": 12,
}


def get_connection() -> sqlite3.Connection:
    """Apre una connessione al database SQLite del progetto."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_csv_files() -> list[Path]:
    """Restituisce tutti i CSV presenti nella cartella raw_csv."""
    return sorted(RAW_CSV_DIR.glob("*.csv"))


def normalize_text(value: object) -> Optional[str]:
    """Normalizza stringhe, trattando vuoti e NaN come None."""
    if value is None:
        return None

    if pd.isna(value):
        return None

    text = str(value).strip()
    if text == "" or text == "-":
        return None

    return text


def parse_float(value: object) -> Optional[float]:
    """Converte numeri in formato italiano ('11569,42') in float."""
    text = normalize_text(value)
    if text is None:
        return None

    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value: object) -> Optional[int]:
    """Converte valori numerici o stringhe numeriche in int."""
    number = parse_float(value)
    if number is None:
        return None
    return int(round(number))


def parse_period_from_label(label: str) -> tuple[str, str]:
    """
    Converte una label tipo 'Aprile 2025' in:
    ('2025-04-01', '2025-04-30')
    """
    cleaned = label.strip().lower()
    parts = cleaned.split()

    if len(parts) != 2:
        raise ValueError(f"Formato periodo non riconosciuto: {label}")

    month_name, year_text = parts
    month = ITALIAN_MONTHS.get(month_name)
    year = int(year_text)

    if month is None:
        raise ValueError(f"Mese italiano non riconosciuto: {label}")

    last_day = calendar.monthrange(year, month)[1]
    period_start = f"{year:04d}-{month:02d}-01"
    period_end = f"{year:04d}-{month:02d}-{last_day:02d}"
    return period_start, period_end


def get_period_from_dataframe(df: pd.DataFrame) -> tuple[str, str]:
    """
    Legge il periodo dalla prima riga del CSV, colonna 'Data'.
    In questi export Beddy la prima riga contiene il totale e il mese.
    """
    if "Data" not in df.columns:
        raise ValueError("Colonna 'Data' non trovata nel CSV.")

    first_value = normalize_text(df.iloc[0]["Data"])
    if first_value is None:
        raise ValueError("La prima riga della colonna 'Data' è vuota.")

    return parse_period_from_label(first_value)


def detect_csv_type(df: pd.DataFrame) -> str:
    """
    Riconosce se il CSV è:
    - channels
    - nationalities
    """
    columns = set(df.columns)

    if "Segmento canale" in columns:
        return "channels"

    if "Segmento nazionalità" in columns:
        return "nationalities"

    raise ValueError("Tipo CSV non riconosciuto.")


def prepare_detail_rows(df: pd.DataFrame, segment_column: str) -> pd.DataFrame:
    """
    Tiene solo le righe di dettaglio, escludendo la riga totale iniziale
    che in Beddy ha il segmento vuoto.
    """
    detail_df = df.copy()
    detail_df = detail_df[detail_df[segment_column].notna()]
    detail_df = detail_df.reset_index(drop=True)
    return detail_df


def delete_existing_channel_rows(
    conn: sqlite3.Connection,
    period_start: str,
    period_end: str,
    source_file: str,
) -> None:
    """Evita duplicati se reimportiamo lo stesso file."""
    conn.execute(
        """
        DELETE FROM beddy_channel_stats
        WHERE period_start = ?
          AND period_end = ?
          AND source_file = ?
        """,
        (period_start, period_end, source_file),
    )


def delete_existing_nationality_rows(
    conn: sqlite3.Connection,
    period_start: str,
    period_end: str,
    source_file: str,
) -> None:
    """Evita duplicati se reimportiamo lo stesso file."""
    conn.execute(
        """
        DELETE FROM beddy_nationality_stats
        WHERE period_start = ?
          AND period_end = ?
          AND source_file = ?
        """,
        (period_start, period_end, source_file),
    )


def import_channels_csv(file_path: Path) -> int:
    """Importa un CSV canali dentro beddy_channel_stats, inclusa la riga Totale finale."""
    df = pd.read_csv(file_path)
    period_start, period_end = get_period_from_dataframe(df)

    total_row = df[df["Data"].astype(str).str.strip().str.lower() == "totale"].copy()

    detail_df = df.copy()
    detail_df = detail_df[detail_df["Segmento canale"].notna()]
    detail_df = detail_df.reset_index(drop=True)

    with get_connection() as conn:
        delete_existing_channel_rows(
            conn=conn,
            period_start=period_start,
            period_end=period_end,
            source_file=file_path.name,
        )

        inserted = 0

        for _, row in detail_df.iterrows():
            conn.execute(
                """
                INSERT INTO beddy_channel_stats (
                    period_start,
                    period_end,
                    channel_name,
                    total_bookings,
                    nights_sold,
                    average_stay,
                    arrivals,
                    departures,
                    revenue,
                    occupancy_rate,
                    incidence_rate,
                    source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    period_start,
                    period_end,
                    normalize_text(row.get("Segmento canale")),
                    parse_int(row.get("Totale prenotazioni")),
                    parse_float(row.get("Notti vendute")),
                    parse_float(row.get("Soggiorno medio")),
                    parse_int(row.get("Arrivi")),
                    parse_int(row.get("Partenze")),
                    parse_float(row.get("Vendita")),
                    parse_float(row.get("Occupazione")),
                    parse_float(row.get("Incidenza (%)")),
                    file_path.name,
                ),
            )
            inserted += 1

        if not total_row.empty:
            total = total_row.iloc[0]

            conn.execute(
                """
                INSERT INTO beddy_channel_stats (
                    period_start,
                    period_end,
                    channel_name,
                    total_bookings,
                    nights_sold,
                    average_stay,
                    arrivals,
                    departures,
                    revenue,
                    occupancy_rate,
                    incidence_rate,
                    source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    period_start,
                    period_end,
                    "Totale",
                    parse_int(total.get("Totale prenotazioni")),
                    parse_float(total.get("Notti vendute")),
                    parse_float(total.get("Soggiorno medio")),
                    parse_int(total.get("Arrivi")),
                    parse_int(total.get("Partenze")),
                    parse_float(total.get("Vendita")),
                    parse_float(total.get("Occupazione")),
                    parse_float(total.get("Incidenza (%)")),
                    file_path.name,
                ),
            )
            inserted += 1

        conn.commit()

    return inserted


def import_nationalities_csv(file_path: Path) -> int:
    """Importa un CSV nazionalità dentro beddy_nationality_stats."""
    df = pd.read_csv(file_path)
    period_start, period_end = get_period_from_dataframe(df)
    detail_df = prepare_detail_rows(df, "Segmento nazionalità")

    with get_connection() as conn:
        delete_existing_nationality_rows(
            conn=conn,
            period_start=period_start,
            period_end=period_end,
            source_file=file_path.name,
        )

        inserted = 0

        for _, row in detail_df.iterrows():
            conn.execute(
                """
                INSERT INTO beddy_nationality_stats (
                    period_start,
                    period_end,
                    nationality,
                    nights_sold,
                    total_bookings,
                    average_stay,
                    arrivals,
                    departures,
                    source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    period_start,
                    period_end,
                    normalize_text(row.get("Segmento nazionalità")),
                    parse_float(row.get("Notti vendute")),
                    parse_int(row.get("Totale prenotazioni")),
                    parse_float(row.get("Soggiorno medio")),
                    parse_int(row.get("Arrivi")),
                    parse_int(row.get("Partenze")),
                    file_path.name,
                ),
            )
            inserted += 1

        conn.commit()

    return inserted


def import_csv_file(file_path: Path) -> None:
    """Instrada il file verso l'import corretto."""
    df = pd.read_csv(file_path)
    csv_type = detect_csv_type(df)

    if csv_type == "channels":
        inserted = import_channels_csv(file_path)
        print(f"[OK] {file_path.name} -> beddy_channel_stats ({inserted} righe)")

    elif csv_type == "nationalities":
        inserted = import_nationalities_csv(file_path)
        print(f"[OK] {file_path.name} -> beddy_nationality_stats ({inserted} righe)")

    else:
        raise ValueError(f"Tipo CSV non gestito: {csv_type}")


def main() -> None:
    files = list_csv_files()

    if not files:
        print("Nessun CSV trovato in data/raw_csv.")
        return

    print(f"CSV trovati: {len(files)}")

    for file_path in files:
        try:
            import_csv_file(file_path)
        except Exception as exc:
            print(f"[ERRORE] {file_path.name} -> {exc}")


if __name__ == "__main__":
    main()