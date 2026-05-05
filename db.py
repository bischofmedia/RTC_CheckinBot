"""
RTC CheckinBot – db.py  v3
Datenbankverbindung und alle DB-Funktionen
"""

import logging
import pymysql
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

log = logging.getLogger("CheckinBot")
BERLIN = ZoneInfo("Europe/Berlin")


def get_connection():
    """Erstellt eine neue DB-Verbindung."""
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


# ─────────────────────────────────────────────
# Saison
# ─────────────────────────────────────────────

def get_active_season_id() -> int | None:
    """Gibt die ID der aktiven Saison zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT season_id AS id FROM seasons WHERE is_active = 1 LIMIT 1")
            row = cur.fetchone()
            return row["id"] if row else None


# ─────────────────────────────────────────────
# Rennkalender
# ─────────────────────────────────────────────

def get_next_monday_race() -> dict | None:
    """Gibt das Rennen zurück, das am nächsten Montag stattfindet."""
    from datetime import timedelta
    today = datetime.now(BERLIN).date()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday = today + timedelta(days=days_until_monday)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rc.*,
                       w.name_de AS weather_name, w.category AS weather_category
                FROM race_calendar rc
                LEFT JOIN gt7_weather_codes w ON w.code = rc.weather_code
                WHERE rc.race_date = %s AND rc.is_pause = 0
                LIMIT 1
            """, (next_monday,))
            return cur.fetchone()


def get_next_future_race() -> dict | None:
    """Gibt das nächste zukünftige Rennen zurück (nicht notwendigerweise nächsten Montag)."""
    today = datetime.now(BERLIN).date()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rc.*,
                       w.name_de AS weather_name, w.category AS weather_category
                FROM race_calendar rc
                LEFT JOIN gt7_weather_codes w ON w.code = rc.weather_code
                WHERE rc.race_date > %s AND rc.is_pause = 0
                ORDER BY rc.race_date ASC
                LIMIT 1
            """, (today,))
            return cur.fetchone()


def get_next_monday_is_pause() -> bool:
    """Gibt True zurück wenn nächsten Montag eine Pause eingetragen ist."""
    today = datetime.now(BERLIN).date()
    days_until_monday = (7 - today.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    from datetime import timedelta
    next_monday = today + timedelta(days=days_until_monday)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM race_calendar
                WHERE race_date = %s AND is_pause = 1
                LIMIT 1
            """, (next_monday,))
            return cur.fetchone() is not None


# ─────────────────────────────────────────────
# Fahrer
# ─────────────────────────────────────────────

def get_driver_by_discord_id(discord_id: str) -> dict | None:
    """Sucht einen Fahrer anhand der Discord-ID."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM drivers
                WHERE discord_id = %s AND is_legacy = 0
                LIMIT 1
            """, (discord_id,))
            return cur.fetchone()


def get_driver_by_nickname(nickname: str) -> dict | None:
    """Sucht einen Fahrer anhand des Server-Nicknamens (discord_name) oder name_history."""
    import json
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Erst direkte Suche
            cur.execute("""
                SELECT * FROM drivers
                WHERE discord_name = %s AND is_legacy = 0
                LIMIT 1
            """, (nickname,))
            row = cur.fetchone()
            if row:
                return row
            # Dann discord_name_history durchsuchen
            cur.execute("""
                SELECT * FROM drivers
                WHERE discord_name_history IS NOT NULL AND is_legacy = 0
            """)
            for driver in cur.fetchall():
                try:
                    history = json.loads(driver["discord_name_history"])
                    if isinstance(history, list) and nickname in history:
                        return driver
                except Exception:
                    pass
            return None


def update_driver_discord_id(driver_id: int, discord_id: str):
    """Trägt die Discord-ID bei einem bekannten Fahrer nach."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE drivers SET discord_id = %s WHERE driver_id = %s
            """, (discord_id, driver_id))
    log.info(f"Discord-ID {discord_id} für driver_id={driver_id} nachgetragen.")


def create_driver(discord_id: str, nickname: str) -> int:
    """Legt einen neuen Fahrer an. PSN-Name = Nickname als Platzhalter."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO drivers (psn_name, discord_id, discord_name, is_active, is_legacy)
                VALUES (%s, %s, %s, 1, 0)
            """, (nickname, discord_id, nickname))
            return cur.lastrowid


def set_driver_active(driver_id: int):
    """Setzt is_active=1 wenn sich ein Fahrer anmeldet."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE drivers SET is_active = 1 WHERE driver_id = %s
            """, (driver_id,))


# ─────────────────────────────────────────────
# Anmeldungen
# ─────────────────────────────────────────────

def get_registration(race_id: int, driver_id: int) -> dict | None:
    """Gibt die aktuelle Anmeldung zurück, nur wenn Fahrer aktiv angemeldet ist."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM checkin_registrations
                WHERE driver_id = %s
                ORDER BY id DESC LIMIT 1
            """, (driver_id,))
            row = cur.fetchone()
            if row and row["action"] != "abgemeldet":
                return row
            return None


def get_all_registrations(race_id: int) -> list:
    """Gibt alle aktuell angemeldeten Fahrer zurück (letzter Eintrag != abgemeldet)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cr.driver_id, cr.source, cr.action, cr.registered_at,
                       d.psn_name, d.discord_id, d.discord_name
                FROM checkin_registrations cr
                JOIN drivers d ON d.driver_id = cr.driver_id
                WHERE cr.id = (
                    SELECT MAX(id) FROM checkin_registrations cr2
                    WHERE cr2.driver_id = cr.driver_id
                )
                AND cr.action != 'abgemeldet'
                ORDER BY cr.registered_at ASC
            """)
            return cur.fetchall()


def get_registration_count(race_id: int) -> int:
    """Gibt die Anzahl der aktuell angemeldeten Fahrer zurück (Anmeldungen minus Abmeldungen)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT driver_id, action FROM checkin_registrations
                ORDER BY id ASC
            """)
            rows = cur.fetchall()
            status = {}
            for row in rows:
                status[row["driver_id"]] = row["action"]
            return sum(1 for a in status.values() if a != "abgemeldet")


def add_registration(race_id: int, driver_id: int, source: str = "manual", action: str = "angemeldet"):
    """Trägt einen Fahrer als angemeldet ein."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkin_registrations (driver_id, source, action, registered_at)
                VALUES (%s, %s, %s, %s)
            """, (driver_id, source, action, datetime.now(BERLIN)))
    set_driver_active(driver_id)


def remove_registration(race_id: int, driver_id: int, source: str = "manual"):
    """Trägt eine Abmelde-Aktion ein."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkin_registrations (driver_id, source, action, registered_at)
                VALUES (%s, %s, 'abgemeldet', %s)
            """, (driver_id, source, datetime.now(BERLIN)))


def clear_log():
    """Leert den kompletten Log."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM checkin_log")
        conn.commit()


def clear_registrations(race_id: int):
    """Löscht alle Anmeldungen für ein Rennen (Reset)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM checkin_registrations")
    log.info("Anmeldeliste geleert.")


# ─────────────────────────────────────────────
# Log
# ─────────────────────────────────────────────

def add_log_entry(driver_id: int, action: str, source: str = "manual"):
    """Schreibt einen Eintrag in checkin_registrations."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkin_registrations (driver_id, source, action, registered_at)
                VALUES (%s, %s, %s, %s)
            """, (driver_id, source, action, datetime.now(BERLIN)))


def get_log_entries(race_id: int) -> list:
    """Gibt alle Einträge chronologisch aus checkin_registrations zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cr.*, d.psn_name, d.discord_name
                FROM checkin_registrations cr
                JOIN drivers d ON d.driver_id = cr.driver_id
                ORDER BY cr.registered_at ASC
            """)
            return cur.fetchall()


# ─────────────────────────────────────────────
# Dauerabo
# ─────────────────────────────────────────────

def get_all_abos() -> list:
    """Gibt alle Fahrer mit Daueranmeldung zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ca.*, d.psn_name, d.discord_id, d.discord_name
                FROM checkin_abo ca
                JOIN drivers d ON d.driver_id = ca.driver_id
                WHERE d.is_legacy = 0
            """)
            return cur.fetchall()


def add_abo(driver_id: int):
    """Trägt einen Fahrer in die Daueranmeldung ein."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT IGNORE INTO checkin_abo (driver_id, created_at)
                VALUES (%s, %s)
            """, (driver_id, datetime.now(BERLIN)))


def remove_abo(driver_id: int):
    """Entfernt einen Fahrer aus der Daueranmeldung."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM checkin_abo WHERE driver_id = %s
            """, (driver_id,))


def has_abo(driver_id: int) -> bool:
    """Gibt True zurück wenn der Fahrer eine Daueranmeldung hat."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT driver_id FROM checkin_abo WHERE driver_id = %s
            """, (driver_id,))
            return cur.fetchone() is not None


# ─────────────────────────────────────────────
# Grid Override
# ─────────────────────────────────────────────

def get_grid_override(race_id: int) -> dict | None:
    """Gibt den manuellen Grid-Override für ein Rennen zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM checkin_grid_override WHERE race_id = %s
            """, (race_id,))
            return cur.fetchone()


def set_grid_override(race_id: int, grid_count: int, discord_id: str):
    """Setzt oder überschreibt den Grid-Override."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkin_grid_override (race_id, grid_count, set_by_discord_id, set_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    grid_count = VALUES(grid_count),
                    set_by_discord_id = VALUES(set_by_discord_id),
                    set_at = VALUES(set_at)
            """, (race_id, grid_count, discord_id, datetime.now(BERLIN)))
    log.info(f"Grid-Override für race_id={race_id}: {grid_count} Grids (gesetzt von {discord_id})")




def get_driver_grid_assignment(driver_id: int, race_id: int) -> dict | None:
    """Gibt die Grid-Einteilung eines Fahrers zurück inkl. Streamer- und Host-Info."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ga.grid_number, ga.position,
                       s.name AS streamer_name, s.url AS streamer_url, s.platform,
                       ga_host.psn_name AS host_name
                FROM grid_assignments ga
                LEFT JOIN grid_assignments ga_streamer ON ga_streamer.grid_number = ga.grid_number
                    AND ga_streamer.is_streamer = 1
                LEFT JOIN streamers s ON s.streamer_id = ga_streamer.streamer_id
                LEFT JOIN grid_assignments ga_host ON ga_host.grid_number = ga.grid_number
                    AND ga_host.is_host = 1 AND ga_host.is_streamer = 0
                WHERE ga.driver_id = %s AND ga.is_streamer = 0
                LIMIT 1
            """, (driver_id,))
            return cur.fetchone()


def get_driver_overall_stats(driver_id: int) -> dict:
    """Gibt allgemeine Fahrer-Statistiken zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Gesamtanzahl Rennen
            cur.execute("""
                SELECT COUNT(*) AS total_races,
                       SUM(CASE WHEN finish_pos_grid = 1 THEN 1 ELSE 0 END) AS total_wins
                FROM race_results
                WHERE driver_id = %s
            """, (driver_id,))
            overall = cur.fetchone()

            return {
                "total_races": overall["total_races"] if overall else 0,
            }

# ─────────────────────────────────────────────
# Standings & Rating (für Status-Button)
# ─────────────────────────────────────────────

def get_driver_current_rating(driver_id: int) -> dict | None:
    """Gibt das aktuelle Rating eines Fahrers zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM v_driver_current_rating
                WHERE driver_id = %s
            """, (driver_id,))
            return cur.fetchone()


def get_driver_season_standings(driver_id: int, season_id: int) -> dict | None:
    """Gibt den Saisonstand eines Fahrers zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM v_season_standings_drivers
                WHERE psn_name = (
                    SELECT psn_name FROM drivers WHERE driver_id = %s
                ) AND season_id = %s
            """, (driver_id, season_id))
            return cur.fetchone()


def get_driver_season_standings_with_drops(driver_id: int, season_id: int) -> dict | None:
    """
    Gibt den Saisonstand eines Fahrers zurück – mit Streichergebnissen.
    Berechnet:
      - races_started: Anzahl gefahrener Rennen
      - active_drops: aktuell aktive Streicher (basierend auf race_number der gefahrenen Rennen)
      - points_total_gross: Punkte ohne Streichung
      - points_dropped: gestrichene Punkte (Summe der schlechtesten N Ergebnisse)
      - points_total_net: Netto-Punkte nach Streichung
      - position: aktuelle Position in der Gesamtwertung (netto)
      - dropped_results: Liste der gestrichenen race_ids und Punkte
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Saison-Daten laden
            cur.execute("""
                SELECT drop_results, drop_after_race_1, drop_after_race_2, drop_after_race_3
                FROM seasons WHERE season_id = %s
            """, (season_id,))
            season = cur.fetchone()
            if not season:
                return None

            # Alle Ergebnisse des Fahrers in dieser Saison (inkl. race_number)
            cur.execute("""
                SELECT rr.race_id, rr.points_total, r.race_number
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                WHERE rr.driver_id = %s AND r.season_id = %s
                ORDER BY r.race_number ASC
            """, (driver_id, season_id))
            results = cur.fetchall()

            # Anzahl stattgefundener Rennen in dieser Saison (race_date <= heute)
            from datetime import date as _date
            cur.execute("""
                SELECT COUNT(*) AS cnt, MAX(race_number) AS max_race_number
                FROM races
                WHERE season_id = %s AND race_date <= %s
            """, (season_id, _date.today()))
            season_races = cur.fetchone()
            races_held = season_races["cnt"] or 0
            max_race_number = season_races["max_race_number"] or 0

            races_started = len(results)
            # DNS/Nicht-Teilnahmen = stattgefundene Rennen minus Teilnahmen → implizite Nullen
            dns_count = max(0, races_held - races_started)
            points_gross = sum(r["points_total"] or 0 for r in results)

            # Aktive Streicher basierend auf stattgefundenen Rennen (nicht nur gefahrenen)
            active_drops = 0
            if season["drop_after_race_1"] and max_race_number >= season["drop_after_race_1"]:
                active_drops = 1
            if season["drop_after_race_2"] and max_race_number >= season["drop_after_race_2"]:
                active_drops = 2
            if season["drop_after_race_3"] and max_race_number >= season["drop_after_race_3"]:
                active_drops = 3
            active_drops = min(active_drops, season["drop_results"] or 0)

            # Alle Ergebnisse inkl. impliziter Nullen für DNS
            all_points = sorted([r["points_total"] or 0 for r in results] + [0] * dns_count)
            dropped_values = all_points[:active_drops]
            points_dropped = sum(dropped_values)
            points_net = points_gross - max(0, sum(v for v in dropped_values if v > 0))
            # Netto = Brutto minus gestrichene Punkte die tatsächlich in results stehen
            # (DNS-Nullen haben keine Punkte zum Abziehen)
            points_net = points_gross - sum(v for v in dropped_values if v > 0)

            # Gesamtposition: alle Fahrer dieser Saison mit Netto-Punkten berechnen
            cur.execute("""
                SELECT rr.driver_id, SUM(rr.points_total) AS gross
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                WHERE r.season_id = %s
                GROUP BY rr.driver_id
            """, (season_id,))
            all_drivers_raw = cur.fetchall()

            # Für jeden Fahrer Netto berechnen (vereinfacht: gross - drops analog zu oben)
            driver_nets = []
            for d in all_drivers_raw:
                cur.execute("""
                    SELECT rr.points_total, r.race_number
                    FROM race_results rr
                    JOIN races r ON r.race_id = rr.race_id
                    WHERE rr.driver_id = %s AND r.season_id = %s
                    ORDER BY rr.points_total ASC
                """, (d["driver_id"], season_id))
                d_results = cur.fetchall()
                d_max_race = max((x["race_number"] for x in d_results), default=0)
                d_drops = 0
                if season["drop_after_race_1"] and d_max_race >= season["drop_after_race_1"]:
                    d_drops = 1
                if season["drop_after_race_2"] and d_max_race >= season["drop_after_race_2"]:
                    d_drops = 2
                if season["drop_after_race_3"] and d_max_race >= season["drop_after_race_3"]:
                    d_drops = 3
                d_drops = min(d_drops, season["drop_results"] or 0)
                d_net = sum(x["points_total"] or 0 for x in d_results[d_drops:])
                driver_nets.append((d["driver_id"], d_net))

            driver_nets.sort(key=lambda x: x[1], reverse=True)
            position = next((i + 1 for i, (did, _) in enumerate(driver_nets) if did == driver_id), None)

            # Gestrichene Ergebnisse: zuerst DNS-Nullen, dann schlechteste echte Ergebnisse
            n_dns_dropped = sum(1 for v in dropped_values if v == 0)
            n_real_dropped = active_drops - n_dns_dropped
            real_sorted = sorted(results, key=lambda r: r["points_total"] or 0)
            dropped_real = real_sorted[:n_real_dropped]

            dropped_results = []
            if n_dns_dropped:
                dropped_results.append({"race_id": None, "points": 0, "race_number": "DNS", "count": n_dns_dropped})
            for r in dropped_real:
                dropped_results.append({"race_id": r["race_id"], "points": r["points_total"], "race_number": r["race_number"]})

            return {
                "races_started": races_started,
                "races_held": races_held,
                "dns_count": dns_count,
                "active_drops": active_drops,
                "points_total_gross": points_gross,
                "points_dropped": points_dropped,
                "points_total_net": points_net,
                "position": position,
                "total_drivers": len(driver_nets),
                "dropped_results": dropped_results,
            }


# ─────────────────────────────────────────────
# Statistik (für Status-Button)
# ─────────────────────────────────────────────

def get_driver_track_stats(driver_id: int, track_id: int) -> dict:
    """Gibt Statistiken eines Fahrers für eine bestimmte Strecke zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Anzahl Rennen auf dieser Strecke
            cur.execute("""
                SELECT COUNT(*) AS race_count
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                WHERE rr.driver_id = %s AND r.track_id = %s
            """, (driver_id, track_id))
            race_count = cur.fetchone()["race_count"]

            # Alle Ergebnisse sortiert nach Saison
            cur.execute("""
                SELECT rr.finish_pos_grid, rr.finish_pos_overall,
                       g.grid_number, rr.time_percent,
                       COALESCE(v.name_short, v.name) AS vehicle_name,
                       s.name AS season_name,
                       s.season_id, r.race_date
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                JOIN seasons s ON s.season_id = r.season_id
                LEFT JOIN vehicles v ON v.vehicle_id = rr.vehicle_id
                LEFT JOIN grids g ON g.grid_id = rr.grid_id
                WHERE rr.driver_id = %s AND r.track_id = %s
                ORDER BY s.season_id ASC, r.race_date ASC
            """, (driver_id, track_id))
            top3 = cur.fetchall()

            # Gefahrene Autos (distinct)
            cur.execute("""
                SELECT DISTINCT v.name AS vehicle_name
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                JOIN vehicles v ON v.vehicle_id = rr.vehicle_id
                WHERE rr.driver_id = %s AND r.track_id = %s
                AND rr.vehicle_id IS NOT NULL
            """, (driver_id, track_id))
            cars = [row["vehicle_name"] for row in cur.fetchall()]

            return {
                "race_count": race_count,
                "top3": top3,
                "cars": cars,
            }


def get_track_overall_stats(track_id: int) -> dict:
    """Gibt allgemeine Streckenstatistiken zurück (Rekord, Anzahl Rennen)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Wie oft wurde die Strecke gefahren
            cur.execute("""
                SELECT COUNT(*) AS total_races
                FROM races
                WHERE track_id = %s
            """, (track_id,))
            total_races = cur.fetchone()["total_races"]

            # Streckenrekord (schnellste Runde aus races-Tabelle)
            cur.execute("""
                SELECT r.fastest_lap_time, d.psn_name, s.name AS season_name
                FROM races r
                JOIN drivers d ON d.driver_id = r.fastest_lap_driver_id
                JOIN seasons s ON s.season_id = r.season_id
                WHERE r.track_id = %s AND r.fastest_lap_time IS NOT NULL
                ORDER BY r.fastest_lap_time ASC
                LIMIT 1
            """, (track_id,))
            record = cur.fetchone()

            return {
                "total_races": total_races,
                "record": record,
            }




def get_track_header_stats(track_id: int, season_class: str = None) -> dict:
    """Gibt erweiterte Streckenstatistiken für den Channel-Header zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            class_filter = "AND s.`class` = %s" if season_class else ""
            params_class = (season_class,) if season_class else ()

            # Wie oft gefahren und letzte Saison
            cur.execute(f"""
                SELECT COUNT(*) AS total_races, MAX(r.race_date) AS last_date,
                       (SELECT s2.name FROM seasons s2
                        JOIN races r2 ON r2.season_id = s2.season_id
                        WHERE r2.track_id = %s {class_filter}
                        ORDER BY r2.race_date DESC LIMIT 1) AS last_season
                FROM races r
                JOIN seasons s ON s.season_id = r.season_id
                WHERE r.track_id = %s {class_filter}
            """, (track_id,) + params_class + (track_id,) + params_class)
            basic = cur.fetchone()

            # Meiste Siege (finish_pos_grid = 1)
            cur.execute(f"""
                SELECT d.psn_name, COUNT(*) AS wins
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                JOIN seasons s ON s.season_id = r.season_id
                JOIN drivers d ON d.driver_id = rr.driver_id
                WHERE r.track_id = %s AND rr.finish_pos_grid = 1 {class_filter}
                GROUP BY rr.driver_id
                ORDER BY wins DESC
                LIMIT 3
            """, (track_id,) + params_class)
            all_winners = cur.fetchall()
            if basic and basic["total_races"] <= 3:
                top_winners = all_winners
            else:
                top_winners = [w for w in all_winners if w["wins"] >= 2]

            # Meist genutzte Fahrzeuge
            cur.execute(f"""
                SELECT COALESCE(v.name_short, v.name) AS vehicle_name, COUNT(*) AS cnt
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                JOIN seasons s ON s.season_id = r.season_id
                JOIN vehicles v ON v.vehicle_id = rr.vehicle_id
                WHERE r.track_id = %s AND rr.vehicle_id IS NOT NULL {class_filter}
                GROUP BY rr.vehicle_id
                ORDER BY cnt DESC
                LIMIT 3
            """, (track_id,) + params_class)
            top_vehicles = cur.fetchall()

            # Schnellste Rennrunde
            cur.execute(f"""
                SELECT r.fastest_lap_time, d.psn_name, s.name AS season_name,
                       gv.game AS game_name, gv.patch AS patch_version, gv.version_id
                FROM races r
                JOIN drivers d ON d.driver_id = r.fastest_lap_driver_id
                JOIN seasons s ON s.season_id = r.season_id
                LEFT JOIN game_versions gv ON gv.version_id = r.version_id
                WHERE r.track_id = %s AND r.fastest_lap_time IS NOT NULL {class_filter}
                ORDER BY r.fastest_lap_time ASC
                LIMIT 1
            """, (track_id,) + params_class)
            record = cur.fetchone()
            # Patch-Version formatieren
            if record and record.get("game_name"):
                game = "GT Sport" if "Sport" in str(record.get("game_name", "")) else "GT7"
                patch = record.get("patch_version", "")
                record["game_str"] = f"{game} Patch {patch}" if patch else game

            return {
                "total_races": basic["total_races"] if basic else 0,
                "last_season": basic["last_season"] if basic else None,
                "top_winners": top_winners or [],
                "top_vehicles": top_vehicles or [],
                "record": record,
            }

# ─────────────────────────────────────────────
# State-Persistenz
# ─────────────────────────────────────────────

def save_state_value(key: str, value: str):
    """Speichert einen State-Wert in der DB."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkin_state (key_name, value)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW()
            """, (key, str(value)))


def load_state_value(key: str, default=None) -> str | None:
    """Lädt einen State-Wert aus der DB."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT value FROM checkin_state WHERE key_name = %s
            """, (key,))
            row = cur.fetchone()
            return row["value"] if row else default


def save_state(state: dict):
    """Speichert den kompletten Bot-State in der DB."""
    for key, value in state.items():
        if value is not None:
            save_state_value(key, str(value))
        else:
            save_state_value(key, "")


def load_state() -> dict:
    """Lädt den kompletten Bot-State aus der DB."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT key_name, value FROM checkin_state")
            rows = cur.fetchall()
            return {row["key_name"]: row["value"] for row in rows}


def get_race_by_id(race_id: int) -> dict | None:
    """Gibt ein Rennen aus race_calendar anhand der ID zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rc.*,
                       w.name_de AS weather_name, w.category AS weather_category
                FROM race_calendar rc
                LEFT JOIN gt7_weather_codes w ON w.code = rc.weather_code
                WHERE rc.id = %s
            """, (race_id,))
            return cur.fetchone()
