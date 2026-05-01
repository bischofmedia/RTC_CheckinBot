"""
RTC CheckinBot – db.py
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
            cur.execute("SELECT id FROM seasons WHERE is_active = 1 LIMIT 1")
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
                SELECT rc.*, s.season,
                       w.name_de AS weather_name, w.category AS weather_category
                FROM race_calendar rc
                LEFT JOIN seasons s ON s.id = rc.season_id
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
                SELECT rc.*, s.season,
                       w.name_de AS weather_name, w.category AS weather_category
                FROM race_calendar rc
                LEFT JOIN seasons s ON s.id = rc.season_id
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
    """Sucht einen Fahrer anhand des Server-Nicknamens (discord_name)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM drivers
                WHERE discord_name = %s AND is_legacy = 0
                LIMIT 1
            """, (nickname,))
            return cur.fetchone()


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
    """Gibt die aktuelle Anmeldung eines Fahrers für ein Rennen zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM checkin_registrations
                WHERE race_id = %s AND driver_id = %s
            """, (race_id, driver_id))
            return cur.fetchone()


def get_all_registrations(race_id: int) -> list:
    """Gibt alle aktuellen Anmeldungen für ein Rennen zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cr.*, d.psn_name, d.discord_id, d.discord_name
                FROM checkin_registrations cr
                JOIN drivers d ON d.driver_id = cr.driver_id
                WHERE cr.race_id = %s
                ORDER BY cr.registered_at ASC
            """, (race_id,))
            return cur.fetchall()


def get_registration_count(race_id: int) -> int:
    """Gibt die Anzahl der aktuellen Anmeldungen zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM checkin_registrations
                WHERE race_id = %s
            """, (race_id,))
            return cur.fetchone()["cnt"]


def add_registration(race_id: int, driver_id: int, source: str = "manual"):
    """Trägt einen Fahrer als angemeldet ein."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT IGNORE INTO checkin_registrations (race_id, driver_id, source, registered_at)
                VALUES (%s, %s, %s, %s)
            """, (race_id, driver_id, source, datetime.now(BERLIN)))
    set_driver_active(driver_id)


def remove_registration(race_id: int, driver_id: int):
    """Entfernt einen Fahrer aus der Anmeldeliste."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM checkin_registrations
                WHERE race_id = %s AND driver_id = %s
            """, (race_id, driver_id))


def clear_registrations(race_id: int):
    """Löscht alle Anmeldungen für ein Rennen (Reset)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM checkin_registrations WHERE race_id = %s
            """, (race_id,))
    log.info(f"Anmeldeliste für race_id={race_id} geleert.")


# ─────────────────────────────────────────────
# Log
# ─────────────────────────────────────────────

def add_log_entry(race_id: int, driver_id: int, action: str):
    """Schreibt einen Eintrag ins Anmelde-Log."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkin_log (race_id, driver_id, action, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (race_id, driver_id, action, datetime.now(BERLIN)))


def get_log_entries(race_id: int) -> list:
    """Gibt alle Log-Einträge für ein Rennen zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cl.*, d.psn_name, d.discord_name
                FROM checkin_log cl
                JOIN drivers d ON d.driver_id = cl.driver_id
                WHERE cl.race_id = %s
                ORDER BY cl.timestamp ASC
            """, (race_id,))
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

            # Top 3 Ergebnisse (nach Grid-Platz, dann Overall)
            cur.execute("""
                SELECT rr.finish_pos_grid, rr.finish_pos_overall,
                       v.name AS vehicle_name, s.season AS season_name,
                       r.race_date
                FROM race_results rr
                JOIN races r ON r.race_id = rr.race_id
                JOIN seasons s ON s.id = r.season_id
                LEFT JOIN vehicles v ON v.vehicle_id = rr.vehicle_id
                WHERE rr.driver_id = %s AND r.track_id = %s
                ORDER BY rr.finish_pos_grid ASC
                LIMIT 3
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
                SELECT r.fastest_lap_time, d.psn_name, s.season AS season_name
                FROM races r
                JOIN drivers d ON d.driver_id = r.fastest_lap_driver_id
                JOIN seasons s ON s.id = r.season_id
                WHERE r.track_id = %s AND r.fastest_lap_time IS NOT NULL
                ORDER BY r.fastest_lap_time ASC
                LIMIT 1
            """, (track_id,))
            record = cur.fetchone()

            return {
                "total_races": total_races,
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
                SELECT rc.*, s.season,
                       w.name_de AS weather_name, w.category AS weather_category
                FROM race_calendar rc
                LEFT JOIN seasons s ON s.id = rc.season_id
                LEFT JOIN gt7_weather_codes w ON w.code = rc.weather_code
                WHERE rc.id = %s
            """, (race_id,))
            return cur.fetchone()
