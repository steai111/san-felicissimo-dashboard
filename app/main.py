from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import sqlite3

from app.config import DB_PATH

from app.database import init_db

from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Data Dashboard")


@app.on_event("startup")
def startup_event() -> None:
    init_db()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_channel_metrics(selected_month: str, selected_year: str, selected_period_mode: str) -> list[dict]:
    """Restituisce Booking e Beddy (PMS) con prenotazioni effettive e incidenza per mese o anno selezionato."""
    with get_connection() as conn:
        cursor = conn.cursor()

        if selected_period_mode == "year":
            cursor.execute(
                """
                WITH yearly AS (
                    SELECT
                        channel_name,
                        SUM(COALESCE(total_bookings, 0) - COALESCE(cancelled_bookings, 0)) AS net_bookings
                    FROM beddy_channel_stats
                    WHERE substr(period_start, 1, 4) = ?
                      AND channel_name IN ('Booking', 'Beddy (PMS)')
                    GROUP BY channel_name
                ),
                grand_total AS (
                    SELECT SUM(net_bookings) AS grand_total_bookings
                    FROM yearly
                )
                SELECT
                    y.channel_name,
                    y.net_bookings AS total_bookings,
                    CASE
                        WHEN g.grand_total_bookings > 0
                        THEN ROUND((y.net_bookings * 100.0) / g.grand_total_bookings, 2)
                        ELSE 0
                    END AS incidence_rate
                FROM yearly y
                CROSS JOIN grand_total g
                ORDER BY
                    CASE
                        WHEN y.channel_name = 'Booking' THEN 1
                        WHEN y.channel_name = 'Beddy (PMS)' THEN 2
                        ELSE 99
                    END
                """,
                (selected_year,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    channel_name,
                    (COALESCE(total_bookings, 0) - COALESCE(cancelled_bookings, 0)) AS total_bookings,
                    incidence_rate
                FROM beddy_channel_stats
                WHERE substr(period_start, 1, 7) = ?
                  AND channel_name IN ('Booking', 'Beddy (PMS)')
                ORDER BY
                    CASE
                        WHEN channel_name = 'Booking' THEN 1
                        WHEN channel_name = 'Beddy (PMS)' THEN 2
                        ELSE 99
                    END
                """,
                (selected_month,),
            )

        rows = cursor.fetchall()

    result = []
    for row in rows:
        if row["channel_name"] == "Booking":
            display_name = "Prenotazioni Booking"
        else:
            display_name = "Prenotazioni Beddy (PMS)"

        result.append(
            {
                "channel_name": display_name,
                "total_bookings": row["total_bookings"] or 0,
                "incidence_rate": row["incidence_rate"] or 0,
            }
        )

    return result


def get_average_stay_total(selected_month: str, selected_year: str, selected_period_mode: str) -> float:
    """Restituisce il soggiorno medio totale per mese o anno selezionato."""
    with get_connection() as conn:
        cursor = conn.cursor()

        if selected_period_mode == "year":
            cursor.execute(
                """
                SELECT
                    SUM(COALESCE(nights_sold, 0)) AS total_nights,
                    SUM(COALESCE(total_bookings, 0)) AS total_bookings
                FROM beddy_channel_stats
                WHERE substr(period_start, 1, 4) = ?
                  AND channel_name = 'Totale'
                """,
                (selected_year,),
            )
        else:
            cursor.execute(
                """
                SELECT average_stay
                FROM beddy_channel_stats
                WHERE substr(period_start, 1, 7) = ?
                  AND channel_name = 'Totale'
                LIMIT 1
                """,
                (selected_month,),
            )

        row = cursor.fetchone()

    if row is None:
        return 0.0

    if selected_period_mode == "year":
        total_nights = row["total_nights"] or 0
        total_bookings = row["total_bookings"] or 0

        if total_bookings == 0:
            return 0.0

        return float(total_nights) / float(total_bookings)

    return row["average_stay"] or 0.0


def get_nationality_presence_metrics(selected_month: str, selected_year: str, selected_period_mode: str) -> list[dict]:
    """Restituisce tutte le nazionalità con la colonna Presenze per il mese o anno selezionato."""
    with get_connection() as conn:
        cursor = conn.cursor()

        if selected_period_mode == "year":
            cursor.execute(
                """
                SELECT
                    nationality,
                    SUM(COALESCE(nights_sold, 0)) AS total_nights
                FROM beddy_nationality_stats
                WHERE substr(period_start, 1, 4) = ?
                  AND nationality IS NOT NULL
                  AND nationality != ''
                GROUP BY nationality
                ORDER BY
                    CASE
                        WHEN nationality = 'Non specificato' THEN 999
                        ELSE 1
                    END,
                    total_nights DESC,
                    nationality ASC
                """,
                (selected_year,),
            )
        else:
            cursor.execute(
                """
                SELECT nationality, nights_sold
                FROM beddy_nationality_stats
                WHERE substr(period_start, 1, 7) = ?
                  AND nationality IS NOT NULL
                  AND nationality != ''
                ORDER BY
                    CASE
                        WHEN nationality = 'Non specificato' THEN 999
                        ELSE 1
                    END,
                    nights_sold DESC,
                    nationality ASC
                """,
                (selected_month,),
            )

        rows = cursor.fetchall()

    result = []
    for row in rows:
        presences = row["total_nights"] if selected_period_mode == "year" else row["nights_sold"]

        if presences is None:
            continue

        formatted_presences = int(round(presences))

        if formatted_presences <= 0:
            continue

        result.append(
            {
                "nationality": row["nationality"],
                "presences": formatted_presences,
            }
        )

    return result


def get_bookings_with_children_count(selected_month: str, selected_year: str, selected_period_mode: str) -> int:
    """Restituisce il numero di prenotazioni con almeno 1 bambino per mese o anno selezionato."""
    with get_connection() as conn:
        cursor = conn.cursor()

        if selected_period_mode == "year":
            if selected_year == "2025":
                cursor.execute(
                    """
                    SELECT COUNT(*) AS total
                    FROM tableau_reservations
                    WHERE source_day >= '2025-04-01'
                      AND source_day <= '2025-04-30'
                      AND children > 0
                    """
                )
                april_row = cursor.fetchone()
                april_total = april_row["total"] or 0 if april_row else 0

                cursor.execute(
                    """
                    SELECT SUM(CAST(metric_value AS INTEGER)) AS total
                    FROM dashboard_metrics
                    WHERE metric_key = 'bookings_with_children_manual'
                      AND substr(period_start, 1, 4) = '2025'
                    """
                )
                manual_row = cursor.fetchone()
                manual_total = manual_row["total"] or 0 if manual_row else 0

                return april_total + manual_total

            cursor.execute(
                """
                SELECT COUNT(DISTINCT reservation_id) AS total
                FROM tableau_reservations
                WHERE substr(source_day, 1, 4) = ?
                  AND children > 0
                """,
                (selected_year,),
            )
            row = cursor.fetchone()
            return row["total"] or 0 if row else 0

        if selected_month == "2025-04":
            cursor.execute(
                """
                SELECT COUNT(DISTINCT reservation_id) AS total
                FROM tableau_reservations
                WHERE substr(source_day, 1, 7) = ?
                  AND children > 0
                """,
                (selected_month,),
            )
            row = cursor.fetchone()
            return row["total"] or 0 if row else 0

        if selected_month.startswith("2026-"):
            cursor.execute(
                """
                SELECT COUNT(DISTINCT reservation_id) AS total
                FROM tableau_reservations
                WHERE substr(source_day, 1, 7) = ?
                  AND children > 0
                """,
                (selected_month,),
            )
            row = cursor.fetchone()
            return row["total"] or 0 if row else 0

        cursor.execute(
            """
            SELECT metric_value
            FROM dashboard_metrics
            WHERE metric_key = 'bookings_with_children_manual'
              AND substr(period_start, 1, 7) = ?
            LIMIT 1
            """,
            (selected_month,),
        )
        row = cursor.fetchone()

    if row is None:
        return 0

    return int(row["metric_value"] or 0)


def get_unit_nights_summary(selected_month: str, selected_year: str, selected_period_mode: str) -> list[dict]:
    """Restituisce notti vendute e libere per ogni unità nel mese o anno selezionato."""
    with get_connection() as conn:
        cursor = conn.cursor()

        if selected_period_mode == "year":
            cursor.execute(
                """
                SELECT
                    unit_label,
                    SUM(is_sold) AS sold_nights,
                    SUM(is_free) AS free_nights
                FROM unit_nights_daily
                WHERE substr(day, 1, 4) = ?
                GROUP BY unit_label
                ORDER BY
                    CASE unit_label
                        WHEN 'Camera 1' THEN 1
                        WHEN 'Camera 2' THEN 2
                        WHEN 'Camera 3' THEN 3
                        WHEN 'Camera 4' THEN 4
                        WHEN 'Appartamento Vite' THEN 5
                        WHEN 'Appartamento Ulivo' THEN 6
                        WHEN 'Appartamento Cipresso' THEN 7
                        ELSE 99
                    END
                """,
                (selected_year,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    unit_label,
                    SUM(is_sold) AS sold_nights,
                    SUM(is_free) AS free_nights
                FROM unit_nights_daily
                WHERE substr(day, 1, 7) = ?
                GROUP BY unit_label
                ORDER BY
                    CASE unit_label
                        WHEN 'Camera 1' THEN 1
                        WHEN 'Camera 2' THEN 2
                        WHEN 'Camera 3' THEN 3
                        WHEN 'Camera 4' THEN 4
                        WHEN 'Appartamento Vite' THEN 5
                        WHEN 'Appartamento Ulivo' THEN 6
                        WHEN 'Appartamento Cipresso' THEN 7
                        ELSE 99
                    END
                """,
                (selected_month,),
            )

        rows = cursor.fetchall()

    result = []
    for row in rows:
        result.append(
            {
                "unit_label": row["unit_label"],
                "sold_nights": row["sold_nights"] or 0,
                "free_nights": row["free_nights"] or 0,
            }
        )

    return result


def get_occupancy_percentage(selected_month: str, selected_year: str, selected_period_mode: str) -> int:
    """Restituisce la percentuale di occupazione basata sulle notti vendute."""
    unit_nights_summary = get_unit_nights_summary(selected_month, selected_year, selected_period_mode)

    total_sold_nights = 0
    total_free_nights = 0

    for item in unit_nights_summary:
        total_sold_nights += int(item["sold_nights"])
        total_free_nights += int(item["free_nights"])

    total_nights = total_sold_nights + total_free_nights
    if total_nights == 0:
        return 0

    return round((total_sold_nights / total_nights) * 100)


def get_website_sessions_count(selected_month: str, selected_year: str, selected_period_mode: str) -> int:
    """Restituisce il numero di sessioni website da GA4 per mese o anno selezionato."""
    with get_connection() as conn:
        cursor = conn.cursor()

        if selected_period_mode == "year":
            cursor.execute(
                """
                SELECT SUM(metric_value) AS total
                FROM ga4_metrics
                WHERE substr(period_start, 1, 4) = ?
                  AND metric_name = 'sessions'
                """,
                (selected_year,),
            )
            row = cursor.fetchone()
            if row is None:
                return 0
            return row["total"] or 0

        cursor.execute(
            """
            SELECT metric_value
            FROM ga4_metrics
            WHERE substr(period_start, 1, 7) = ?
              AND metric_name = 'sessions'
            LIMIT 1
            """,
            (selected_month,),
        )
        row = cursor.fetchone()

    if row is None:
        return 0

    return row["metric_value"] or 0


def build_comparison_metric(label: str, value_a: float, value_b: float) -> dict:
    """Crea una metrica di confronto tra due valori."""
    difference = value_a - value_b

    if value_b != 0:
        percentage_change = round((difference / value_b) * 100, 2)
    else:
        percentage_change = None

    return {
        "label": label,
        "value_a": value_a,
        "value_b": value_b,
        "difference": difference,
        "percentage_change": percentage_change,
    }


def build_unit_nights_comparison(unit_nights_a: list[dict], unit_nights_b: list[dict]) -> list[dict]:
    """Confronta le notti vendute/libere per unità tra due periodi."""
    units_map_a = {
        item["unit_label"]: item
        for item in unit_nights_a
    }

    units_map_b = {
        item["unit_label"]: item
        for item in unit_nights_b
    }

    ordered_units = [
        "Camera 1",
        "Camera 2",
        "Camera 3",
        "Camera 4",
        "Appartamento Vite",
        "Appartamento Ulivo",
        "Appartamento Cipresso",
    ]

    result = []

    for unit_label in ordered_units:
        item_a = units_map_a.get(unit_label, {})
        item_b = units_map_b.get(unit_label, {})

        sold_a = int(item_a.get("sold_nights", 0) or 0)
        sold_b = int(item_b.get("sold_nights", 0) or 0)

        free_a = int(item_a.get("free_nights", 0) or 0)
        free_b = int(item_b.get("free_nights", 0) or 0)

        result.append(
            {
                "unit_label": unit_label,
                "sold": build_comparison_metric("Vendute", sold_a, sold_b),
                "free": build_comparison_metric("Libere", free_a, free_b),
            }
        )

    return result

def build_nationalities_comparison(nationalities_a: list[dict], nationalities_b: list[dict]) -> list[dict]:
    """Confronta le presenze per nazionalità tra due periodi."""
    map_a = {
        item["nationality"]: int(item["presences"] or 0)
        for item in nationalities_a
    }

    map_b = {
        item["nationality"]: int(item["presences"] or 0)
        for item in nationalities_b
    }

    all_nationalities = sorted(
        set(map_a.keys()) | set(map_b.keys()),
        key=lambda nationality: (
            -max(map_a.get(nationality, 0), map_b.get(nationality, 0)),
            nationality,
        ),
    )

    result = []

    for nationality in all_nationalities:
        value_a = map_a.get(nationality, 0)
        value_b = map_b.get(nationality, 0)

        if value_a == 0 and value_b == 0:
            continue

        result.append(
            build_comparison_metric(
                nationality,
                value_a,
                value_b,
            )
        )

    return result


def build_nationality_summary(nationality_metrics: list[dict]) -> dict:
    """Calcola percentuale italiani/stranieri dalle presenze per nazionalità."""
    italians_total = 0
    foreigners_total = 0

    for item in nationality_metrics:
        presences = int(item["presences"] or 0)

        if item["nationality"] == "Italia":
            italians_total += presences
        else:
            foreigners_total += presences

    total = italians_total + foreigners_total

    if total > 0:
        italians_percentage = round((italians_total / total) * 100)
        foreigners_percentage = round((foreigners_total / total) * 100)
    else:
        italians_percentage = 0
        foreigners_percentage = 0

    return {
        "italians_total": italians_total,
        "foreigners_total": foreigners_total,
        "italians_percentage": italians_percentage,
        "foreigners_percentage": foreigners_percentage,
    }


@app.get("/api/compare", response_class=JSONResponse)
def compare_api(
    year_a: str = "2026",
    month_a: str = "2026-03",
    mode_a: str = "month",
    year_b: str = "2025",
    month_b: str = "2025-04",
    mode_b: str = "month",
) -> dict:
    channel_metrics_a = get_channel_metrics(month_a, year_a, mode_a)
    channel_metrics_b = get_channel_metrics(month_b, year_b, mode_b)

    booking_a = channel_metrics_a[0]["total_bookings"] if len(channel_metrics_a) > 0 else 0
    booking_b = channel_metrics_b[0]["total_bookings"] if len(channel_metrics_b) > 0 else 0

    beddy_a = channel_metrics_a[1]["total_bookings"] if len(channel_metrics_a) > 1 else 0
    beddy_b = channel_metrics_b[1]["total_bookings"] if len(channel_metrics_b) > 1 else 0

    average_stay_a = round(float(get_average_stay_total(month_a, year_a, mode_a)), 1)
    average_stay_b = round(float(get_average_stay_total(month_b, year_b, mode_b)), 1)

    children_a = get_bookings_with_children_count(month_a, year_a, mode_a)
    children_b = get_bookings_with_children_count(month_b, year_b, mode_b)

    occupancy_a = get_occupancy_percentage(month_a, year_a, mode_a)
    occupancy_b = get_occupancy_percentage(month_b, year_b, mode_b)

    website_a = get_website_sessions_count(month_a, year_a, mode_a)
    website_b = get_website_sessions_count(month_b, year_b, mode_b)

    unit_nights_a = get_unit_nights_summary(month_a, year_a, mode_a)
    unit_nights_b = get_unit_nights_summary(month_b, year_b, mode_b)
    unit_nights_comparison = build_unit_nights_comparison(unit_nights_a, unit_nights_b)

    nationalities_a = get_nationality_presence_metrics(month_a, year_a, mode_a)
    nationalities_b = get_nationality_presence_metrics(month_b, year_b, mode_b)
    nationalities_comparison = build_nationalities_comparison(nationalities_a, nationalities_b)

    return {
        "period_a": {
            "year": year_a,
            "month": month_a,
            "mode": mode_a,
        },
        "period_b": {
            "year": year_b,
            "month": month_b,
            "mode": mode_b,
        },
        "metrics": [
            build_comparison_metric("Prenotazioni Booking", booking_a, booking_b),
            build_comparison_metric("Prenotazioni Beddy", beddy_a, beddy_b),
            build_comparison_metric("Soggiorno medio", average_stay_a, average_stay_b),
            build_comparison_metric("Prenotazioni con bambini", children_a, children_b),
            build_comparison_metric("Occupazione", occupancy_a, occupancy_b),
            build_comparison_metric("Visite website", website_a, website_b),
        ],
        "unit_nights": unit_nights_comparison,
        "nationalities": nationalities_comparison,
    }


@app.get("/api/dashboard", response_class=JSONResponse)
def dashboard_api(
    year: str = "2025",
    month: str = "2025-04",
    mode: str = "month",
) -> dict:
    channel_metrics = get_channel_metrics(month, year, mode)
    average_stay_total = get_average_stay_total(month, year, mode)
    nationality_metrics = get_nationality_presence_metrics(month, year, mode)
    nationality_summary = build_nationality_summary(nationality_metrics)
    children_bookings_count = get_bookings_with_children_count(month, year, mode)
    unit_nights_summary = get_unit_nights_summary(month, year, mode)
    website_sessions_count = get_website_sessions_count(month, year, mode)
    occupancy_percentage = get_occupancy_percentage(month, year, mode)

    total_sold_nights = 0
    total_free_nights = 0

    for item in unit_nights_summary:
        total_sold_nights += int(item["sold_nights"])
        total_free_nights += int(item["free_nights"])

    total_nights = total_sold_nights + total_free_nights
    if total_nights > 0:
        sold_percentage = round((total_sold_nights / total_nights) * 100)
        free_percentage = round((total_free_nights / total_nights) * 100)
    else:
        sold_percentage = 0
        free_percentage = 0

    return {
        "year": year,
        "month": month,
        "mode": mode,
        "summary": {
            "booking_count": channel_metrics[0]["total_bookings"] if len(channel_metrics) > 0 else 0,
            "beddy_pms_count": channel_metrics[1]["total_bookings"] if len(channel_metrics) > 1 else 0,
            "average_stay_total": round(float(average_stay_total), 1),
            "children_bookings_count": children_bookings_count,
            "occupancy_percentage": occupancy_percentage,
            "website_sessions_count": website_sessions_count,
        },
        "channels": channel_metrics,
        "unit_nights": {
            "rows": unit_nights_summary,
            "totals": {
                "sold_nights": total_sold_nights,
                "free_nights": total_free_nights,
            },
            "occupancy": {
                "sold_percentage": sold_percentage,
                "free_percentage": free_percentage,
            },
        },
        "nationalities": nationality_metrics,
        "nationality_summary": nationality_summary,
    }


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> str:
    selected_year = request.query_params.get("year", "2025")
    selected_month = request.query_params.get("month", "2025-04")
    selected_period_mode = request.query_params.get("mode", "month")

    year_options = [
        ("2025", "2025"),
        ("2026", "2026"),
    ]

    month_name_map = {
        "01": "Gennaio",
        "02": "Febbraio",
        "03": "Marzo",
        "04": "Aprile",
        "05": "Maggio",
        "06": "Giugno",
        "07": "Luglio",
        "08": "Agosto",
        "09": "Settembre",
        "10": "Ottobre",
        "11": "Novembre",
        "12": "Dicembre",
    }

    available_months_by_year = {
        "2025": [
            "2025-04",
            "2025-05",
            "2025-06",
            "2025-07",
            "2025-08",
            "2025-09",
            "2025-10",
            "2025-11",
            "2025-12",
        ],
        "2026": ["2026-03", "2026-04"],
    }

    month_options = [
        (month_key, f"{month_name_map[month_key[5:7]]} {month_key[:4]}")
        for month_key in available_months_by_year.get(selected_year, [])
    ]

    if selected_month not in [value for value, _ in month_options]:
        selected_month = month_options[0][0] if month_options else ""

    month_label_map = dict(month_options)

    if selected_period_mode == "year":
        selected_month_label = f"Anno intero {selected_year}"
    else:
        selected_month_label = month_label_map.get(selected_month, selected_month)

    channel_metrics = get_channel_metrics(selected_month, selected_year, selected_period_mode)
    average_stay_total = get_average_stay_total(selected_month, selected_year, selected_period_mode)
    nationality_metrics = get_nationality_presence_metrics(selected_month, selected_year, selected_period_mode)
    children_bookings_count = get_bookings_with_children_count(selected_month, selected_year, selected_period_mode)
    unit_nights_summary = get_unit_nights_summary(selected_month, selected_year, selected_period_mode)
    website_sessions_count = get_website_sessions_count(selected_month, selected_year, selected_period_mode)
    occupancy_percentage = get_occupancy_percentage(selected_month, selected_year, selected_period_mode)

    channel_cards_html = ""
    for item in channel_metrics:
        channel_cards_html += f"""
        <div class="card">
            <div class="label">{item['channel_name']}</div>
            <div class="value">{item['total_bookings']}</div>
            <div class="sub">
                Incidenza: {item['incidence_rate']:.2f}%
            </div>
        </div>
        """

    nationality_rows_html = ""
    italians_total = 0
    foreigners_total = 0

    for item in nationality_metrics:
        nationality_rows_html += f"""
        <tr>
            <td>{item['nationality']}</td>
            <td>{item['presences']}</td>
        </tr>
        """

        if item["nationality"] == "Italia":
            italians_total += int(item["presences"])
        else:
            foreigners_total += int(item["presences"])

    nationalities_total = italians_total + foreigners_total

    if nationalities_total > 0:
        italians_percentage = round((italians_total / nationalities_total) * 100)
        foreigners_percentage = round((foreigners_total / nationalities_total) * 100)
    else:
        italians_percentage = 0
        foreigners_percentage = 0

    nationality_rows_html += f"""
    <tr>
        <td><strong>Totale</strong></td>
        <td><strong>Italiani {italians_percentage}% · Stranieri {foreigners_percentage}%</strong></td>
    </tr>
    """

    unit_rows_html = ""
    total_sold_nights = 0
    total_free_nights = 0

    for item in unit_nights_summary:
        total_sold_nights += int(item["sold_nights"])
        total_free_nights += int(item["free_nights"])

        unit_rows_html += f"""
        <tr>
            <td>
                <span class="unit-label-desktop">{item['unit_label']}</span>
                <span class="unit-label-mobile">
                    {"App. Vite" if item['unit_label'] == "Appartamento Vite" else
                     "App. Ulivo" if item['unit_label'] == "Appartamento Ulivo" else
                     "App. Cipresso" if item['unit_label'] == "Appartamento Cipresso" else
                     item['unit_label']}
                </span>
            </td>
            <td>{item['sold_nights']}</td>
            <td>{item['free_nights']}</td>
        </tr>
        """

    total_nights = total_sold_nights + total_free_nights

    if total_nights > 0:
        sold_percentage = round((total_sold_nights / total_nights) * 100)
        free_percentage = round((total_free_nights / total_nights) * 100)
    else:
        sold_percentage = 0
        free_percentage = 0

    unit_rows_html += f"""
    <tr>
        <td><strong>Totale</strong></td>
        <td><strong>{total_sold_nights}</strong></td>
        <td><strong>{total_free_nights}</strong></td>
    </tr>
    <tr>
        <td><strong>Occupazione</strong></td>
        <td><strong>{sold_percentage}%</strong></td>
        <td><strong>{free_percentage}%</strong></td>
    </tr>
    """

    return f"""
    <!DOCTYPE html>
    <html lang="it">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>San Felicissimo Data Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 32px;
                background: #f5f7fb;
                color: #1f2937;
            }}
            h1 {{
                margin-bottom: 10px;
                font-size: 40px;
            }}
            .subtitle {{
                color: #6b7280;
                margin-bottom: 28px;
                font-size: 22px;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(2, minmax(320px, 1fr));
                gap: 24px;
                margin-bottom: 28px;
            }}
            .card {{
                background: white;
                border-radius: 18px;
                padding: 28px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
            }}
            .label {{
                font-size: 22px;
                color: #6b7280;
                margin-bottom: 12px;
                line-height: 1.3;
            }}
            .value {{
                font-size: 52px;
                font-weight: bold;
                margin-bottom: 12px;
                line-height: 1.1;
            }}
            .sub {{
                font-size: 22px;
                color: #374151;
                line-height: 1.5;
            }}
            .table-card {{
                background: white;
                border-radius: 18px;
                padding: 28px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
                overflow-x: auto;
                margin-bottom: 28px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                text-align: left;
                padding: 14px 10px;
                border-bottom: 1px solid #e5e7eb;
            }}
            th {{
                font-size: 22px;
                color: #6b7280;
            }}
            td {{
                font-size: 24px;
            }}
            .unit-label-mobile {{
                display: none;
            }}
            @media (max-width: 768px) {{
                .grid {{
                    grid-template-columns: 1fr;
                }}
                .table-card {{
                    padding: 16px;
                }}
                th, td {{
                    padding: 10px 6px;
                }}
                .unit-label-desktop {{
                    display: none;
                }}
                .unit-label-mobile {{
                    display: inline;
                }}
            }}
        </style>
    </head>
    <body>
        <h1>San Felicissimo Data Dashboard</h1>
        <div class="subtitle">Periodo selezionato: {selected_month_label}</div>

        <form method="get" style="margin-bottom: 28px; display: flex; gap: 18px; align-items: center; flex-wrap: wrap;">
            <div>
                <label for="year" style="font-size: 22px; color: #6b7280; margin-right: 12px;">
                    Seleziona anno:
                </label>
                <select
                    id="year"
                    name="year"
                    onchange="this.form.submit()"
                    style="font-size: 20px; padding: 10px 14px; border-radius: 10px; border: 1px solid #d1d5db;"
                >
                    {''.join(
                        f'<option value="{value}" {"selected" if value == selected_year else ""}>{label}</option>'
                        for value, label in year_options
                    )}
                </select>
            </div>

            <div>
                <label for="mode" style="font-size: 22px; color: #6b7280; margin-right: 12px;">
                    Visualizzazione:
                </label>
                <select
                    id="mode"
                    name="mode"
                    onchange="this.form.submit()"
                    style="font-size: 20px; padding: 10px 14px; border-radius: 10px; border: 1px solid #d1d5db;"
                >
                    <option value="month" {"selected" if selected_period_mode == "month" else ""}>Mese</option>
                    <option value="year" {"selected" if selected_period_mode == "year" else ""}>Anno intero</option>
                </select>
            </div>

            <div style="display: flex; align-items: center; gap: 12px; flex-wrap: nowrap;">
                <label for="month" style="font-size: 22px; color: #6b7280; margin-right: 12px;">
                    Seleziona mese:
                </label>
                <select
                    id="month"
                    name="month"
                    onchange="this.form.submit()"
                    {"disabled" if selected_period_mode == "year" else ""}
                    style="font-size: 20px; padding: 10px 14px; border-radius: 10px; border: 1px solid #d1d5db;"
                >
                    {''.join(
                        f'<option value="{value}" {"selected" if value == selected_month else ""}>{label}</option>'
                        for value, label in month_options
                    )}
                </select>
            </div>
        </form>

        <div class="grid">
            {channel_cards_html}

            <div class="card">
                <div class="label">Soggiorno medio totale</div>
                <div class="value">{average_stay_total:.1f}</div>
                <div class="sub">Calcolato dai dati totali del periodo</div>
            </div>

            <div class="card">
                <div class="label">Prenotazioni con bambini</div>
                <div class="value">{children_bookings_count}</div>
                <div class="sub">{selected_month_label}</div>
            </div>

            <div class="card">
                <div class="label">Occupazione</div>
                <div class="value">{occupancy_percentage}%</div>
                <div class="sub">Notti vendute sul totale</div>
            </div>

            <div class="card">
                <div class="label">Visite al website</div>
                <div class="value">{website_sessions_count}</div>
                <div class="sub">Sessioni GA4 · {selected_month_label}</div>
            </div>
        </div>

        <div class="table-card" style="margin-bottom: 24px;">
            <div class="label" style="margin-bottom: 16px;">Notti per unità</div>
            <table>
                <thead>
                    <tr>
                        <th>Unità</th>
                        <th>Vendute</th>
                        <th>Libere</th>
                    </tr>
                </thead>
                <tbody>
                    {unit_rows_html}
                </tbody>
            </table>
        </div>

        <div class="table-card">
            <div class="label" style="margin-bottom: 16px;">Presenze per nazionalità</div>
            <table>
                <thead>
                    <tr>
                        <th>Nazionalità</th>
                        <th>Presenze</th>
                    </tr>
                </thead>
                <tbody>
                    {nationality_rows_html}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)