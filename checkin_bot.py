"""
RTC CheckinBot
Ersetzt Apollo für die Anmeldung zum wöchentlichen RTC GT7 Rennen.

Block A: Imports, Configuration, State Management, Flask Health Server
"""

import asyncio
import json
import logging
import math
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import pymysql
from dotenv import load_dotenv
from flask import Flask

load_dotenv()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("CheckinBot")

BERLIN = ZoneInfo("Europe/Berlin")

# ─────────────────────────────────────────────
# Constants / Paths
# ─────────────────────────────────────────────
STATE_FILE        = Path("state.json")
EVENT_LOG_FILE    = Path("event_log.txt")
DISCORD_LOG_FILE  = Path("discord_log.txt")
ANMELDUNGEN_FILE  = Path("anmeldungen.txt")
ABO_FILE          = Path("anmelde-abo.txt")

DISCORD_API = "https://discord.com/api/v10"

# ─────────────────────────────────────────────
# Environment variable helpers
# ─────────────────────────────────────────────
def _env(key: str, default=None) -> str:
    return os.environ.get(key, default)

def _env_msg(key: str, default: str = "") -> str:
    val = os.environ.get(key, default)
    return val.replace("\\n", "\n") if val else default

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

# ─────────────────────────────────────────────
# Static environment variables
# ─────────────────────────────────────────────
DISCORD_TOKEN                  = _env("DISCORD_TOKEN_CHECKINBOT", "")
DISCORD_TOKEN_LOBBYCODEGRABBER = _env("DISCORD_TOKEN_LOBBYCODEGRABBER", "")
USER_ID_ORGA       = [u.strip() for u in _env("USER_ID_ORGA", "").split(";") if u.strip()]
DISCORD_GUILD_ID   = _env("DISCORD_GUILD_ID", "")
TESTMODUS          = _env("TESTMODUS", "false").lower() == "true"

CHAN_CHECKIN = _env("CHAN_CHECKIN", "")
CHAN_LOG     = _env("CHAN_LOG", "")
CHAN_NEWS    = _env("CHAN_NEWS", "")
CHAN_CODES   = _env("CHAN_CODES", "")
CHAN_ORDERS  = _env("CHAN_ORDERS") or _env("CHAN_LOG", "")
CHAN_ORGA    = _env("CHAN_ORGA", "")

DRIVERS_PER_GRID        = _env_int("DRIVERS_PER_GRID", 15)
MAX_GRIDS               = _env_int("MAX_GRIDS", 4)
GOOGLE_SHEETS_ID        = _env("GOOGLE_SHEETS_ID", "")
GOOGLE_CREDENTIALS_FILE = _env("GOOGLE_CREDENTIALS_FILE", "credentials.json")
CMD_SCAN_INTERVAL_SECONDS = _env_int("CMD_SCAN_INTERVAL_SECONDS", 10)
ENABLE_MULTILANGUAGE    = _env_int("ENABLE_MULTILANGUAGE", 0)

# DB
DB_HOST     = _env("DB_HOST", "")
DB_USER     = _env("DB_USER", "")
DB_PASSWORD = _env("DB_PASSWORD", "")
DB_NAME     = _env("DB_NAME", "")
CURRENT_SEASON_ID = _env_int("CURRENT_SEASON_ID", 16)

# Message templates
MSG_HILFETEXT          = _env_msg("MSG_HILFETEXT", "Kein Hilfetext konfiguriert.")
MSG_LOBBYCODES         = _env_msg("MSG_LOBBYCODES", "Lobby-Codes")
MSG_EXTRA_GRID_TEXT    = _env_msg("MSG_EXTRA_GRID_TEXT", "")
MSG_EXTRA_GRID_TEXT_EN = _env_msg("MSG_EXTRA_GRID_TEXT_EN", "")
MSG_GRID_FULL_TEXT     = _env_msg("MSG_GRID_FULL_TEXT", "")
MSG_GRID_FULL_TEXT_EN  = _env_msg("MSG_GRID_FULL_TEXT_EN", "")
MSG_MOVED_UP_SINGLE    = _env_msg("MSG_MOVED_UP_SINGLE", "")
MSG_MOVED_UP_SINGLE_EN = _env_msg("MSG_MOVED_UP_SINGLE_EN", "")
MSG_MOVED_UP_MULTI     = _env_msg("MSG_MOVED_UP_MULTI", "")
MSG_MOVED_UP_MULTI_EN  = _env_msg("MSG_MOVED_UP_MULTI_EN", "")
MSG_SUNDAY_TEXT        = _env_msg("MSG_SUNDAY_TEXT", "")
MSG_SUNDAY_TEXT_EN     = _env_msg("MSG_SUNDAY_TEXT_EN", "")
MSG_WAITLIST_SINGLE    = _env_msg("MSG_WAITLIST_SINGLE", "")
MSG_WAITLIST_SINGLE_EN = _env_msg("MSG_WAITLIST_SINGLE_EN", "")
MSG_WAITLIST_MULTI     = _env_msg("MSG_WAITLIST_MULTI", "")
MSG_WAITLIST_MULTI_EN  = _env_msg("MSG_WAITLIST_MULTI_EN", "")
MSG_NEW_EVENT          = _env_msg("MSG_NEW_EVENT", "")
MSG_NEW_EVENT_EN       = _env_msg("MSG_NEW_EVENT_EN", "")
MSG_GRID_CHANGE_TEXT   = _env_msg("MSG_GRID_CHANGE_TEXT", "")
MSG_GRID_CHANGE_TEXT_EN = _env_msg("MSG_GRID_CHANGE_TEXT_EN", "")

# ─────────────────────────────────────────────
# var_* keys
# ─────────────────────────────────────────────
VAR_KEYS = [
    "var_ENABLE_EXTRA_GRID",
    "var_ENABLE_MOVED_UP_MSG",
    "var_ENABLE_NEWS_CLEANUP",
    "var_ENABLE_SUNDAY_MSG",
    "var_ENABLE_WAITLIST_MSG",
    "var_EXTRA_GRID_THRESHOLD",
    "var_POLL_INTERVAL_SECONDS",
    "var_REGISTRATION_END_TIME",
    "var_SET_MIN_GRIDS_MSG",
    "var_ENABLE_EXTRA_GRID_MSG",
    "var_ENABLE_GRID_FULL_MSG",
    "var_SET_MSG_MOVED_UP_TEXT",
    "var_SET_NEW_EVENT_MSG",
]

VAR_ENV_MAP = {k: k[4:] for k in VAR_KEYS}

# ─────────────────────────────────────────────
# Default state
# ─────────────────────────────────────────────
DEFAULT_STATE: dict = {
    "event_title": "",
    "event_datetime": "",
    "new_event": 0,
    "registration_end_monday": "",
    "drivers": [],
    "driver_status": {},
    "manual_grids": None,
    "man_lock": False,
    "sunday_lock": False,
    "last_grid_count": 0,
    "grid_lock_override": False,
    "log_id": "",
    "checkin_message_id": "",      # ID der aktuellen Checkin-Nachricht in CHAN_CHECKIN
    "last_sync_sheets": "",
    "sunday_msg_sent": False,
    "registration_end_logged": False,
    "registration_end_locked": False,
    "ignored_drivers": [],
    "driver_discord_cache": {},
    "var_ENABLE_EXTRA_GRID": 0,
    "var_ENABLE_MOVED_UP_MSG": 1,
    "var_ENABLE_NEWS_CLEANUP": 1,
    "var_ENABLE_SUNDAY_MSG": 1,
    "var_ENABLE_WAITLIST_MSG": 1,
    "var_EXTRA_GRID_THRESHOLD": 3,
    "var_POLL_INTERVAL_SECONDS": 60,
    "var_REGISTRATION_END_TIME": "23:59",
    "var_SET_MIN_GRIDS_MSG": 2,
    "var_ENABLE_EXTRA_GRID_MSG": 1,
    "var_ENABLE_GRID_FULL_MSG": 1,
    "var_SET_MSG_MOVED_UP_TEXT": 1,
    "var_SET_NEW_EVENT_MSG": 1,
}

state: dict = {}


def _coerce_var(var_key: str, raw: str):
    int_keys = {
        "var_ENABLE_EXTRA_GRID", "var_ENABLE_MOVED_UP_MSG",
        "var_ENABLE_NEWS_CLEANUP", "var_ENABLE_SUNDAY_MSG",
        "var_ENABLE_WAITLIST_MSG", "var_EXTRA_GRID_THRESHOLD",
        "var_POLL_INTERVAL_SECONDS",
        "var_SET_MIN_GRIDS_MSG", "var_ENABLE_EXTRA_GRID_MSG",
        "var_ENABLE_GRID_FULL_MSG", "var_SET_MSG_MOVED_UP_TEXT",
        "var_SET_NEW_EVENT_MSG",
    }
    if var_key in int_keys:
        try:
            return int(raw)
        except (ValueError, TypeError):
            return 0
    return str(raw)


def load_state() -> None:
    global state
    if STATE_FILE.exists():
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            state = {**DEFAULT_STATE, **loaded}
            log.info("state.json geladen.")
            return
        except Exception as e:
            log.warning(f"state.json fehlerhaft, neu initialisieren: {e}")
    state = dict(DEFAULT_STATE)
    for var_key, env_key in VAR_ENV_MAP.items():
        val = os.environ.get(env_key)
        if val is not None:
            state[var_key] = _coerce_var(var_key, val)
    save_state()


def save_state() -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        tmp.replace(STATE_FILE)
    except Exception as e:
        log.error(f"Fehler beim Speichern der state.json: {e}")


def cfg(key: str):
    return state.get(f"var_{key}", DEFAULT_STATE.get(f"var_{key}"))


# ─────────────────────────────────────────────
# File helpers
# ─────────────────────────────────────────────
def ensure_files() -> None:
    for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE, ABO_FILE):
        if not fp.exists():
            fp.write_text("", encoding="utf-8")


def append_event_log(line: str) -> None:
    with EVENT_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def read_event_log() -> str:
    return EVENT_LOG_FILE.read_text(encoding="utf-8") if EVENT_LOG_FILE.exists() else ""


def read_discord_log() -> str:
    return DISCORD_LOG_FILE.read_text(encoding="utf-8") if DISCORD_LOG_FILE.exists() else ""


def write_discord_log(content: str) -> None:
    DISCORD_LOG_FILE.write_text(content, encoding="utf-8")


def write_anmeldungen(drivers: list) -> None:
    ANMELDUNGEN_FILE.write_text("\n".join(drivers), encoding="utf-8")


# ─────────────────────────────────────────────
# Daueranmeldung (anmelde-abo.txt)
# ─────────────────────────────────────────────
def read_abo() -> list:
    """Gibt die Liste der dauerhaft angemeldeten PSN-Namen zurück."""
    if not ABO_FILE.exists():
        return []
    return [line.strip() for line in ABO_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]


def add_abo(psn_name: str) -> bool:
    """Fügt PSN-Name zum Abo hinzu. Gibt True zurück wenn neu eingetragen."""
    names = read_abo()
    if psn_name in names:
        return False
    names.append(psn_name)
    ABO_FILE.write_text("\n".join(names), encoding="utf-8")
    log.info(f"Daueranmeldung hinzugefügt: {psn_name}")
    return True


def remove_abo(psn_name: str) -> bool:
    """Entfernt PSN-Name aus Abo. Gibt True zurück wenn gefunden und entfernt."""
    names = read_abo()
    if psn_name not in names:
        return False
    names = [n for n in names if n != psn_name]
    ABO_FILE.write_text("\n".join(names), encoding="utf-8")
    log.info(f"Daueranmeldung entfernt: {psn_name}")
    return True


# ─────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────
def now_berlin() -> datetime:
    return datetime.now(BERLIN)


def ts_str() -> str:
    n = now_berlin()
    days = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
    return f"{days[n.weekday()]} {n.strftime('%H:%M')}"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _next_monday_date(reference: datetime) -> str:
    days_ahead = (7 - reference.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    target = (reference + timedelta(days=days_ahead)).date()
    return target.isoformat()


def set_registration_end_monday() -> None:
    if state.get("registration_end_monday"):
        return
    monday_str = _next_monday_date(now_berlin())
    state["registration_end_monday"] = monday_str
    log.info(f"Anmeldeschluss-Montag gesetzt: {monday_str}")


def registration_end_passed() -> bool:
    if state.get("registration_end_locked"):
        return True
    ref_str = state.get("registration_end_monday", "")
    if not ref_str:
        return False
    n = now_berlin()
    try:
        ref_date = datetime.strptime(ref_str, "%Y-%m-%d").date()
    except ValueError:
        return False
    if n.date() != ref_date:
        return False
    end_str = str(state.get("var_REGISTRATION_END_TIME", "23:59"))
    try:
        h, m = map(int, end_str.split(":"))
    except Exception:
        return False
    return n.hour > h or (n.hour == h and n.minute >= m)


def is_sunday_lock_time() -> bool:
    n = now_berlin()
    if n.weekday() == 6 and n.hour >= 18:
        return True
    if n.weekday() == 0:
        return True
    return False


def is_tuesday_reset_time() -> bool:
    """True dienstags zwischen 09:00 und 09:15 Berliner Zeit."""
    n = now_berlin()
    return n.weekday() == 1 and n.hour == 9 and n.minute < 15


def _is_monday_gridchange_time() -> bool:
    n = now_berlin()
    if n.weekday() != 0:
        return False
    return (n.hour == 18) or (n.hour == 19) or (n.hour == 20 and n.minute <= 30)


# ─────────────────────────────────────────────
# Datenbank-Helfer
# ─────────────────────────────────────────────
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_next_race() -> dict | None:
    """
    Gibt das Rennen zurück, das am kommenden Montag stattfindet.
    Wird dienstags aufgerufen – nächster Montag = heute + 6 Tage.
    """
    next_monday = date.today() + timedelta(days=6)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT race_number, race_date, track_name, laps,
                           time_of_day, weather_code, is_pause
                    FROM race_calendar
                    WHERE season_id = %s AND race_date = %s
                    LIMIT 1
                """, (CURRENT_SEASON_ID, next_monday))
                return cur.fetchone()
    except Exception as e:
        log.error(f"DB-Fehler get_next_race: {e}")
        return None


# ─────────────────────────────────────────────
# Flask Health Server
# ─────────────────────────────────────────────
flask_app = Flask("CheckinBotHealth")


@flask_app.route("/")
@flask_app.route("/dashboard")
def dashboard():
    grids = state.get("last_grid_count", 0)
    return build_html_dashboard(grids), 200


@flask_app.route("/health")
def health():
    return "RTC CheckinBot running", 200


def start_flask() -> None:
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)


def launch_flask_thread() -> None:
    t = threading.Thread(target=start_flask, daemon=True, name="FlaskHealth")
    t.start()
    log.info("Flask Health Server gestartet.")


# ─────────────────────────────────────────────
# Low-level Discord REST helpers
# ─────────────────────────────────────────────
def _auth(token: str) -> dict:
    return {"Authorization": f"Bot {token}", "Content-Type": "application/json"}


async def discord_get(session: aiohttp.ClientSession, path: str, token: str) -> dict | list | None:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.get(url, headers=_auth(token)) as r:
            if r.status == 200:
                return await r.json()
            log.warning(f"GET {path} -> {r.status}")
            return None
    except Exception as e:
        log.error(f"discord_get {path}: {e}")
        return None


async def discord_post(session: aiohttp.ClientSession, path: str, token: str, payload: dict) -> dict | None:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.post(url, headers=_auth(token), json=payload) as r:
            if r.status in (200, 201):
                return await r.json()
            text = await r.text()
            log.warning(f"POST {path} -> {r.status}: {text[:200]}")
            return None
    except Exception as e:
        log.error(f"discord_post {path}: {e}")
        return None


async def discord_patch(session: aiohttp.ClientSession, path: str, token: str, payload: dict) -> dict | None:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.patch(url, headers=_auth(token), json=payload) as r:
            if r.status == 200:
                return await r.json()
            text = await r.text()
            log.warning(f"PATCH {path} -> {r.status}: {text[:200]}")
            return None
    except Exception as e:
        log.error(f"discord_patch {path}: {e}")
        return None


async def discord_delete(session: aiohttp.ClientSession, path: str, token: str) -> bool:
    url = f"{DISCORD_API}{path}"
    try:
        async with session.delete(url, headers=_auth(token)) as r:
            return r.status in (200, 204)
    except Exception as e:
        log.error(f"discord_delete {path}: {e}")
        return False


async def get_channel_messages(
    session: aiohttp.ClientSession,
    channel_id: str,
    token: str,
    limit: int = 200,
) -> list:
    messages = []
    last_id = None
    while len(messages) < limit:
        batch_size = min(100, limit - len(messages))
        path = f"/channels/{channel_id}/messages?limit={batch_size}"
        if last_id:
            path += f"&before={last_id}"
        batch = await discord_get(session, path, token)
        if not batch:
            break
        messages.extend(batch)
        if len(batch) < batch_size:
            break
        last_id = batch[-1]["id"]
    return messages


async def delete_all_messages(session: aiohttp.ClientSession, channel_id: str, token: str) -> None:
    """Löscht ALLE Nachrichten in einem Kanal (für wöchentliches Channel-Leeren)."""
    messages = await get_channel_messages(session, channel_id, token)
    for msg in messages:
        await discord_delete(session, f"/channels/{channel_id}/messages/{msg['id']}", token)
        await asyncio.sleep(0.3)  # Rate-limit-freundlich


async def delete_all_bot_messages(
    session: aiohttp.ClientSession,
    channel_id: str,
    token: str,
    bot_user_id: str | None = None,
) -> None:
    messages = await get_channel_messages(session, channel_id, token)
    for msg in messages:
        author_id = msg.get("author", {}).get("id", "")
        if bot_user_id is None or author_id == bot_user_id:
            await discord_delete(session, f"/channels/{channel_id}/messages/{msg['id']}", token)
            await asyncio.sleep(0.3)


async def get_bot_user_id(session: aiohttp.ClientSession, token: str) -> str | None:
    data = await discord_get(session, "/users/@me", token)
    return data.get("id") if data else None


async def get_display_name(session: aiohttp.ClientSession, user_id: str, fallback: str) -> str:
    if not DISCORD_GUILD_ID:
        return fallback
    data = await discord_get(
        session,
        f"/guilds/{DISCORD_GUILD_ID}/members/{user_id}",
        DISCORD_TOKEN,
    )
    if data:
        return data.get("nick") or data.get("user", {}).get("global_name") or fallback
    return fallback


async def get_guild_member(session: aiohttp.ClientSession, user_id: str) -> dict | None:
    """Gibt das vollständige Member-Objekt zurück."""
    if not DISCORD_GUILD_ID:
        return None
    return await discord_get(
        session,
        f"/guilds/{DISCORD_GUILD_ID}/members/{user_id}",
        DISCORD_TOKEN,
    )


# ─────────────────────────────────────────────
# Grid Calculation
# ─────────────────────────────────────────────
def calculate_grids(driver_count: int) -> int:
    if driver_count <= 0:
        return 0
    return min(math.ceil(driver_count / DRIVERS_PER_GRID), MAX_GRIDS)


def grid_capacity(grids: int) -> int:
    return grids * DRIVERS_PER_GRID


def recalculate_grids(driver_count: int) -> int:
    override   = state.get("grid_lock_override", False)
    sunday_lock = state.get("sunday_lock", False)
    man_lock   = state.get("man_lock", False)
    if (sunday_lock or man_lock) and not override:
        return int(state.get("last_grid_count", 0))
    new_count = calculate_grids(driver_count)
    state["last_grid_count"] = new_count
    if override:
        state["grid_lock_override"] = False
    return new_count


def check_extra_grid(driver_count: int, current_grids: int) -> bool:
    if not int(cfg("ENABLE_EXTRA_GRID")):
        return False
    capacity = grid_capacity(current_grids)
    waitlist_count = max(0, driver_count - capacity)
    threshold = int(cfg("EXTRA_GRID_THRESHOLD"))
    if waitlist_count >= threshold and current_grids < MAX_GRIDS:
        state["grid_lock_override"] = True
        return True
    return False


def classify_drivers(drivers: list, grids: int) -> dict:
    capacity = grid_capacity(grids)
    return {name: ("grid" if i < capacity else "waitlist") for i, name in enumerate(drivers)}


# ─────────────────────────────────────────────
# Driver lookup in DB_drvr (Google Sheets)
# ─────────────────────────────────────────────
def _get_gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def _load_db_drvr_rows() -> list:
    """
    Liest DB_drvr Sheet ab Zeile 8 (Header in Zeile 7).
    Gibt Liste von Dicts zurück: {psn, discord_name, discord_id}
    Spaltenindizes (0-basiert, ab Spalte B=1):
      Spalte B (idx 0) = PSN-Name
      Spalte J (idx 8) = Discord Name (Servernickname)
      Letzte Spalte    = Discord-ID (DC, idx 104 relativ zu C5 → wir lesen A:DC)
    """
    if not GOOGLE_SHEETS_ID:
        return []
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)
        ws = sh.worksheet("DB_drvr")
        # Lese B8:DC500 – Zeile 7 ist Header, ab Zeile 8 Daten
        rows = ws.get("B8:DC500")
        result = []
        for row in rows:
            if not row or not row[0].strip():
                continue
            psn          = row[0].strip()   # Spalte B
            discord_name = row[8].strip() if len(row) > 8 else ""   # Spalte J
            discord_id   = row[104].strip() if len(row) > 104 else ""  # Spalte DC
            result.append({"psn": psn, "discord_name": discord_name, "discord_id": discord_id})
        log.info(f"DB_drvr geladen: {len(result)} Einträge.")
        return result
    except Exception as e:
        log.error(f"DB_drvr Lesefehler: {e}")
        return []


def _lookup_driver(discord_id: str, server_nick: str, rows: list) -> dict | None:
    """
    Sucht einen Fahrer in den DB_drvr-Zeilen.
    Priorität: Discord-ID → Server-Nickname.
    Gibt {psn, discord_name, discord_id, row_index} zurück oder None.
    """
    # 1. Suche per Discord-ID
    if discord_id:
        for i, row in enumerate(rows):
            if row["discord_id"] == discord_id:
                return {**row, "row_index": i}
    # 2. Suche per Server-Nickname (case-insensitive)
    if server_nick:
        for i, row in enumerate(rows):
            if row["discord_name"].lower() == server_nick.lower():
                return {**row, "row_index": i}
    return None


def _write_discord_id_to_sheet(row_index: int, discord_id: str) -> None:
    """Trägt Discord-ID in DB_drvr Spalte DC ein (Zeilennummer = row_index + 8)."""
    if not GOOGLE_SHEETS_ID:
        return
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)
        ws = sh.worksheet("DB_drvr")
        sheet_row = row_index + 8
        ws.update(range_name=f"DC{sheet_row}", values=[[discord_id]])
        log.info(f"Discord-ID {discord_id} in DB_drvr Zeile {sheet_row} eingetragen.")
    except Exception as e:
        log.error(f"Fehler beim Schreiben der Discord-ID: {e}")


async def resolve_driver_psn(
    session: aiohttp.ClientSession,
    discord_user_id: str,
) -> tuple[str | None, str]:
    """
    Löst Discord-User-ID → PSN-Name auf.
    Liest DB_drvr und wendet die Identifikationslogik an.
    Gibt (psn_name, status) zurück.
    status: 'found' | 'id_written' | 'nick_mismatch_fixed' | 'not_found'
    """
    member = await get_guild_member(session, discord_user_id)
    if not member:
        return None, "not_found"

    server_nick = member.get("nick") or member.get("user", {}).get("username", "")

    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, _load_db_drvr_rows)

    found = _lookup_driver(discord_user_id, server_nick, rows)

    if found:
        # Discord-ID war leer → eintragen
        if not found["discord_id"]:
            await loop.run_in_executor(
                None, _write_discord_id_to_sheet, found["row_index"], discord_user_id
            )
            return found["psn"], "id_written"
        # Nickname stimmt nicht überein → korrigieren (kein Schreibvorgang nötig, nur Log)
        if found["discord_name"] and found["discord_name"].lower() != server_nick.lower():
            log.info(
                f"Nickname-Abweichung: DB hat '{found['discord_name']}', "
                f"Discord zeigt '{server_nick}' – PSN: {found['psn']}"
            )
            return found["psn"], "nick_mismatch_fixed"
        return found["psn"], "found"

    # Nicht gefunden → Servernickname eintragen (kein Write, nur ORGA-Meldung)
    return None, "not_found"


async def refresh_driver_discord_map(session: aiohttp.ClientSession) -> None:
    """Baut den driver_discord_cache aus DB_drvr auf (psn → discord_id)."""
    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, _load_db_drvr_rows)
    cache = {}
    for row in rows:
        if row["psn"] and row["discord_id"]:
            cache[row["psn"]] = row["discord_id"]
    state["driver_discord_cache"] = cache
    log.info(f"Discord-ID-Cache: {len(cache)} Einträge.")


def _mention(psn_name: str) -> str:
    uid = state.get("driver_discord_cache", {}).get(psn_name)
    return f"<@{uid}>" if uid else psn_name


async def _send_dm(session: aiohttp.ClientSession, psn_name: str, text: str) -> None:
    uid = state.get("driver_discord_cache", {}).get(psn_name)
    if not uid:
        return
    try:
        dm = await discord_post(session, "/users/@me/channels", DISCORD_TOKEN, {"recipient_id": uid})
        if dm:
            channel_id = dm.get("id")
            if channel_id:
                await discord_post(session, f"/channels/{channel_id}/messages", DISCORD_TOKEN, {"content": text})
    except Exception as e:
        log.error(f"DM Fehler für {psn_name}: {e}")


# ─────────────────────────────────────────────
# Checkin Channel – Event-Nachricht mit Buttons
# ─────────────────────────────────────────────

def _build_checkin_components() -> list:
    """Erstellt die Button-Zeilen für die Checkin-Nachricht."""
    return [
        {
            "type": 1,  # Action Row
            "components": [
                {
                    "type": 2, "style": 3, "label": "✅ Anmelden",
                    "custom_id": "checkin_register",
                },
                {
                    "type": 2, "style": 1, "label": "🔔 Daueranmeldung",
                    "custom_id": "checkin_register_abo",
                },
            ],
        },
        {
            "type": 1,
            "components": [
                {
                    "type": 2, "style": 4, "label": "❌ Abmelden",
                    "custom_id": "checkin_unregister",
                },
                {
                    "type": 2, "style": 2, "label": "🔕 Dauerabmeldung",
                    "custom_id": "checkin_unregister_abo",
                },
            ],
        },
        {
            "type": 1,
            "components": [
                {
                    "type": 2, "style": 2, "label": "📋 Status",
                    "custom_id": "checkin_status",
                },
            ],
        },
    ]


def _build_driver_list_text() -> str:
    """Formatiert die aktuelle Fahrerliste für die Checkin-Nachricht."""
    drivers = state.get("drivers", [])
    grids   = int(state.get("last_grid_count", 0))
    capacity = grid_capacity(grids)

    if not drivers:
        return "_Noch keine Anmeldungen._"

    lines = []
    for i, name in enumerate(drivers):
        num = i + 1
        if i < capacity:
            lines.append(f"{num}. {name}")
        else:
            lines.append(f"~~{num}. {name}~~ _(Warteliste)_")
    return "\n".join(lines)


def _build_checkin_embed(race: dict | None, is_pause: bool = False) -> dict:
    """Baut das Embed für die Checkin-Nachricht."""
    if is_pause or race is None:
        return {
            "title": "😴 Nächste Woche kein Rennen",
            "description": "Diese Woche ist Pause. Wir sehen uns wieder nächste Woche!",
            "color": 0x95a5a6,
        }

    reg_end = str(state.get("var_REGISTRATION_END_TIME", "23:59"))
    race_date_str = race["race_date"].strftime("%d.%m.%Y") if hasattr(race["race_date"], "strftime") else str(race["race_date"])

    driver_list = _build_driver_list_text()

    return {
        "title": f"🏁 Rennen {race['race_number']} – {race['track_name']}",
        "color": 0xe74c3c,
        "fields": [
            {"name": "📅 Datum", "value": f"{race_date_str} · **20:45 Uhr**", "inline": True},
            {"name": "🔄 Runden", "value": str(race["laps"]), "inline": True},
            {"name": "🌤️ Tageszeit", "value": race["time_of_day"], "inline": True},
            {"name": "🌦️ Wetter", "value": race["weather_code"], "inline": True},
            {"name": f"👥 Anmeldungen ({len(state.get('drivers', []))})", "value": driver_list, "inline": False},
        ],
        "footer": {"text": f"Anmeldeschluss: Montag {reg_end} Uhr · Änderungen bis dahin möglich."},
    }


async def post_checkin_message(session: aiohttp.ClientSession, race: dict | None) -> None:
    """
    Löscht den kompletten CHAN_CHECKIN und postet die neue Checkin-Nachricht.
    Wird dienstags um 09:00 aufgerufen.
    """
    target_channel = CHAN_CHECKIN if not TESTMODUS else CHAN_CHECKIN

    log.info("CHAN_CHECKIN wird geleert …")
    await delete_all_messages(session, target_channel, DISCORD_TOKEN)
    await asyncio.sleep(1)

    is_pause = (race is None) or bool(race.get("is_pause"))
    embed = _build_checkin_embed(race, is_pause)
    payload: dict = {"embeds": [embed]}

    if not is_pause:
        payload["components"] = _build_checkin_components()

    msg = await discord_post(session, f"/channels/{target_channel}/messages", DISCORD_TOKEN, payload)
    if msg:
        state["checkin_message_id"] = msg["id"]
        save_state()
        log.info(f"Checkin-Nachricht gepostet: {msg['id']}")


async def update_checkin_message(session: aiohttp.ClientSession) -> None:
    """Aktualisiert die bestehende Checkin-Nachricht (Fahrerliste hat sich geändert)."""
    msg_id = state.get("checkin_message_id", "")
    if not msg_id:
        return

    race = get_next_race()  # Fallback – im Normalfall haben wir die Daten schon im State
    embed = _build_checkin_embed(race, race is None or bool(race.get("is_pause")))
    payload = {
        "embeds": [embed],
        "components": _build_checkin_components(),
    }
    await discord_patch(session, f"/channels/{CHAN_CHECKIN}/messages/{msg_id}", DISCORD_TOKEN, payload)


# ─────────────────────────────────────────────
# Button Interaction Handler
# ─────────────────────────────────────────────

async def handle_interaction(session: aiohttp.ClientSession, interaction: dict) -> None:
    """
    Verarbeitet einen eingehenden Button-Klick.
    Discord sendet Interactions an einen Webhook-Endpunkt.
    Da wir den klassischen Gateway-Ansatz nutzen, pollen wir Interactions
    über die REST-API nicht – stattdessen wird dieser Handler vom Gateway aufgerufen.
    HINWEIS: Für Button-Interactions ist ein Interaction-Webhook oder Gateway nötig.
    Wir verwenden hier den Gateway (discord.py-losen Ansatz via aiohttp + REST).
    """
    custom_id    = interaction.get("data", {}).get("custom_id", "")
    discord_user = interaction.get("member", {}).get("user", {}) or interaction.get("user", {})
    discord_id   = discord_user.get("id", "")
    interaction_id    = interaction.get("id", "")
    interaction_token = interaction.get("token", "")

    async def ephemeral_reply(text: str) -> None:
        """Schickt eine ephemerale Antwort auf die Interaction."""
        await discord_post(
            session,
            f"/interactions/{interaction_id}/{interaction_token}/callback",
            DISCORD_TOKEN,
            {
                "type": 4,  # CHANNEL_MESSAGE_WITH_SOURCE
                "data": {
                    "content": text,
                    "flags": 64,  # EPHEMERAL
                },
            },
        )

    if not discord_id:
        return

    # PSN-ID ermitteln
    psn_name, resolve_status = await resolve_driver_psn(session, discord_id)

    # Nicht gefunden → ORGA informieren und User bescheid geben
    if psn_name is None and custom_id not in ("checkin_status",):
        member = await get_guild_member(session, discord_id)
        server_nick = member.get("nick") or member.get("user", {}).get("username", "Unbekannt") if member else "Unbekannt"

        if CHAN_ORGA:
            await discord_post(
                session,
                f"/channels/{CHAN_ORGA}/messages",
                DISCORD_TOKEN,
                {"content": (
                    f"⚠️ **Nicht zuordenbarer Fahrer hat Button geklickt!**\n"
                    f"Discord-ID: `{discord_id}` · Servernick: `{server_nick}`\n"
                    f"Bitte in DB_drvr prüfen und ggf. eintragen."
                )},
            )
        await ephemeral_reply(
            "❌ Dein Account konnte nicht zugeordnet werden. "
            "Bitte wende dich an die Organisation."
        )
        return

    drivers    = list(state.get("drivers", []))
    is_in_list = psn_name in drivers if psn_name else False
    reg_end    = registration_end_passed()

    # ── Anmelden ────────────────────────────────────────────────────────────
    if custom_id == "checkin_register":
        if reg_end:
            await ephemeral_reply("⛔ Der Anmeldeschluss ist bereits vorbei.")
            return
        if is_in_list:
            await ephemeral_reply(f"✅ Du bist bereits angemeldet als **{psn_name}**.")
            return
        drivers.append(psn_name)
        state["drivers"] = drivers
        new_grids = recalculate_grids(len(drivers))
        state["driver_status"] = classify_drivers(drivers, new_grids)
        write_anmeldungen(drivers)
        append_event_log(f"{ts_str()} 🟢 {psn_name}")
        save_state()
        await sync_to_sheets(session, "update")
        await update_checkin_message(session)
        _rebuild_discord_log(new_grids)
        await _refresh_chan_log(session)
        pos = drivers.index(psn_name) + 1
        capacity = grid_capacity(new_grids)
        if pos <= capacity:
            await ephemeral_reply(f"✅ Du bist angemeldet! **{psn_name}** · Platz {pos} · Grid {math.ceil(pos / DRIVERS_PER_GRID)}")
        else:
            await ephemeral_reply(f"✅ Angemeldet auf der **Warteliste** · Position {pos - capacity}. **{psn_name}**")

    # ── Abmelden ────────────────────────────────────────────────────────────
    elif custom_id == "checkin_unregister":
        if reg_end:
            await ephemeral_reply("⛔ Der Anmeldeschluss ist bereits vorbei. Melde dich bitte bei der Organisation.")
            return
        if not is_in_list:
            await ephemeral_reply("ℹ️ Du bist aktuell nicht angemeldet.")
            return
        drivers.remove(psn_name)
        state["drivers"] = drivers
        new_grids = recalculate_grids(len(drivers))
        state["driver_status"] = classify_drivers(drivers, new_grids)
        write_anmeldungen(drivers)
        append_event_log(f"{ts_str()} 🔴 {psn_name}")
        save_state()
        await sync_to_sheets(session, "update")
        await update_checkin_message(session)
        _rebuild_discord_log(new_grids)
        await _refresh_chan_log(session)
        await ephemeral_reply(f"👋 Du wurdest abgemeldet. **{psn_name}**")

    # ── Daueranmeldung ──────────────────────────────────────────────────────
    elif custom_id == "checkin_register_abo":
        was_new = add_abo(psn_name)
        # Direkt auch für dieses Rennen anmelden, falls noch nicht drin
        if not is_in_list and not reg_end:
            drivers.append(psn_name)
            state["drivers"] = drivers
            new_grids = recalculate_grids(len(drivers))
            state["driver_status"] = classify_drivers(drivers, new_grids)
            write_anmeldungen(drivers)
            append_event_log(f"{ts_str()} 🟢 {psn_name} (Abo)")
            save_state()
            await sync_to_sheets(session, "update")
            await update_checkin_message(session)
            _rebuild_discord_log(new_grids)
            await _refresh_chan_log(session)
        if was_new:
            await ephemeral_reply(
                f"🔔 **Daueranmeldung aktiviert!** Du wirst ab sofort jeden Dienstag automatisch angemeldet. **{psn_name}**"
            )
        else:
            await ephemeral_reply(f"ℹ️ Du bist bereits dauerhaft angemeldet. **{psn_name}**")

    # ── Dauerabmeldung ──────────────────────────────────────────────────────
    elif custom_id == "checkin_unregister_abo":
        was_removed = remove_abo(psn_name)
        # Auch aus der aktuellen Liste entfernen, falls drin
        if is_in_list and not reg_end:
            drivers.remove(psn_name)
            state["drivers"] = drivers
            new_grids = recalculate_grids(len(drivers))
            state["driver_status"] = classify_drivers(drivers, new_grids)
            write_anmeldungen(drivers)
            append_event_log(f"{ts_str()} 🔴 {psn_name} (Abo-Ende)")
            save_state()
            await sync_to_sheets(session, "update")
            await update_checkin_message(session)
            _rebuild_discord_log(new_grids)
            await _refresh_chan_log(session)
        if was_removed:
            await ephemeral_reply(
                f"🔕 **Daueranmeldung deaktiviert.** Du wirst nicht mehr automatisch angemeldet. **{psn_name}**"
            )
        else:
            await ephemeral_reply(f"ℹ️ Du hattest keine aktive Daueranmeldung. **{psn_name}**")

    # ── Status ──────────────────────────────────────────────────────────────
    elif custom_id == "checkin_status":
        await handle_status_button(session, ephemeral_reply, psn_name, discord_id)


async def handle_status_button(
    session: aiohttp.ClientSession,
    reply_fn,
    psn_name: str | None,
    discord_id: str,
) -> None:
    """Erstellt die Status-Antwort für den Status-Button."""
    if psn_name is None:
        await reply_fn("❌ Dein Account konnte nicht zugeordnet werden.")
        return

    drivers      = state.get("drivers", [])
    abo_list     = read_abo()
    is_registered = psn_name in drivers
    has_abo      = psn_name in abo_list
    reg_end      = registration_end_passed()
    reg_end_time = str(state.get("var_REGISTRATION_END_TIME", "23:59"))

    if not is_registered:
        abo_hint = "\n🔔 Daueranmeldung ist aktiv." if has_abo else ""
        await reply_fn(f"📋 **Status für {psn_name}**\n❌ Nicht angemeldet.{abo_hint}")
        return

    # Position und Grid
    pos      = drivers.index(psn_name) + 1
    grids    = int(state.get("last_grid_count", 0))
    capacity = grid_capacity(grids)
    on_grid  = pos <= capacity

    if on_grid:
        grid_num = math.ceil(pos / DRIVERS_PER_GRID)
        status_line = f"✅ Angemeldet · Platz {pos} · **Grid {grid_num}**"
    else:
        wl_pos = pos - capacity
        status_line = f"✅ Angemeldet · **Warteliste** Position {wl_pos}"

    # Grid-Infos aus Google Sheets lesen (Host, Streamer, Lobbycode)
    grid_info = ""
    if on_grid:
        grid_info = await _get_grid_info(psn_name) if GOOGLE_SHEETS_ID else ""

    abo_hint = "\n🔔 Daueranmeldung aktiv." if has_abo else ""
    deadline_hint = (
        f"\n⏰ Änderungen sind noch bis Montag **{reg_end_time} Uhr** möglich."
        if not reg_end else ""
    )

    lines = [f"📋 **Status für {psn_name}**", status_line]
    if grid_info:
        lines.append(grid_info)
    lines.append(abo_hint + deadline_hint)

    await reply_fn("\n".join(l for l in lines if l))


async def _get_grid_info(psn_name: str) -> str:
    """Liest Host, Streamer und Lobbycode aus dem Grids-Sheet."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: _read_grid_info_sync(psn_name))
        return result
    except Exception as e:
        log.error(f"Grid-Info Fehler: {e}")
        return ""


def _read_grid_info_sync(psn_name: str) -> str:
    """Synchron: Liest Grids-Sheet und findet Grid/Host/Streamer/Lobbycode für psn_name."""
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)
        ws = sh.worksheet("Grids")  # Blattname ggf. anpassen

        # Lese das gesamte Sheet (kompakt)
        all_data = ws.get_all_values()

        # Grid-Blöcke identifizieren: Zeilen mit "Grid-X" in Spalte C
        # Struktur aus CSV: Spalten 1,6,11,16 = Grid-Header, Spalte 2=Driver, 1=Host/Streamer
        # Wir suchen den PSN-Namen in Driver-Spalten (B, G, L, Q → Index 1,6,11,16)
        grid_col_offsets = [1, 6, 11, 16]  # 0-basiert: Spalten B, G, L, Q

        found_grid = None
        found_host = None
        found_streamer = None
        found_lobbycode = None

        for grid_block_idx, driver_col in enumerate(grid_col_offsets):
            grid_name = None
            lobby_code = None
            host = None
            streamer = None

            for row_idx, row in enumerate(all_data):
                if len(row) <= driver_col:
                    continue

                cell = row[driver_col].strip()

                # Grid-Name in Header-Zeile erkennen
                if "Grid-" in cell or "Warteliste" in cell:
                    grid_name = cell
                    # Lobbycode steht in der gleichen Zeile, Spalte driver_col+1
                    if len(row) > driver_col + 1:
                        lobby_code = row[driver_col + 1].strip()
                    continue

                # Host/Streamer-Zeile
                dr_col = driver_col + 1 if driver_col < len(row) else -1
                if dr_col >= 0 and len(row) > dr_col:
                    role = row[dr_col].strip()
                    if role == "Streamer":
                        streamer = cell
                    elif role == "Host":
                        host = cell

                # Fahrer-Match
                if cell == psn_name and grid_name:
                    found_grid = grid_name
                    found_host = host
                    found_streamer = streamer
                    found_lobbycode = lobby_code
                    break

            if found_grid:
                break

        if not found_grid:
            return ""

        lines = [f"🏎️ **{found_grid}**"]
        if found_host:
            lines.append(f"👑 Host: **{found_host}**")
        if found_streamer:
            lines.append(f"📺 Streamer: **{found_streamer}**")
        if found_lobbycode:
            lines.append(f"🔑 Lobbycode: `{found_lobbycode}`")

        return "\n".join(lines)

    except Exception as e:
        log.error(f"_read_grid_info_sync Fehler: {e}")
        return ""


# ─────────────────────────────────────────────
# Event log delta processing
# ─────────────────────────────────────────────
def process_driver_changes(new_drivers: list, old_drivers: list, new_grids: int, reg_end: bool) -> dict:
    capacity  = grid_capacity(new_grids)
    old_set   = set(old_drivers)
    new_set   = set(new_drivers)
    old_status = state.get("driver_status", {})

    added = []; removed = []; moved_up = []; waitlisted = []; ignored = []

    for name in old_drivers:
        if name not in new_set:
            removed.append(name)
            prev = old_status.get(name)
            if prev == "grid":
                append_event_log(f"{ts_str()} 🟢 -> 🔴 {name}")
            elif prev == "waitlist":
                append_event_log(f"{ts_str()} 🟡 -> 🔴 {name}")
            else:
                append_event_log(f"{ts_str()} 🔴 {name}")

    for i, name in enumerate(new_drivers):
        cur  = "grid" if i < capacity else "waitlist"
        prev = old_status.get(name)
        if name not in old_set:
            if reg_end:
                already_ignored = name in state.get("ignored_drivers", [])
                ignored.append(name)
                if not already_ignored:
                    append_event_log(f"{ts_str()} 🔴🔴 {name}")
                continue
            if cur == "grid":
                added.append(name)
                append_event_log(f"{ts_str()} {'🔴 -> ' if prev else ''}🟢 {name}")
            else:
                waitlisted.append(name)
                append_event_log(f"{ts_str()} {'🔴 -> ' if prev else ''}🟡 {name}")
        else:
            if prev == "waitlist" and cur == "grid":
                moved_up.append(name)
                append_event_log(f"{ts_str()} 🟡 -> 🟢 {name}")
            elif prev == "grid" and cur == "waitlist":
                waitlisted.append(name)
                append_event_log(f"{ts_str()} 🟢 -> 🟡 {name}")

    return {"added": added, "removed": removed, "moved_up": moved_up, "waitlisted": waitlisted, "ignored": ignored}


# ─────────────────────────────────────────────
# discord_log.txt / CHAN_LOG
# ─────────────────────────────────────────────
def build_discord_log(grids: int) -> str:
    raw_lines = read_event_log().splitlines()
    return "\n".join(line.replace("\\", "") for line in raw_lines)


def build_clean_log(grids: int, username: str = "") -> str:
    current_status = state.get("driver_status", {})
    last_ts: dict = {}
    for line in read_event_log().splitlines():
        for name in current_status:
            if name in line:
                last_ts[name] = line
    out = [f"{ts_str()} ⚙️ Clean Log ({username})"]
    for name, status in current_status.items():
        emoji = "🟢" if status == "grid" else "🟡"
        ts_line = last_ts.get(name, "")
        prefix = ts_line[:8] if len(ts_line) >= 8 else ts_str()
        out.append(f"{prefix} {emoji} {name}")
    return "\n".join(out)


def _rebuild_discord_log(grids: int) -> None:
    content = build_discord_log(grids)
    write_discord_log(content)


def _registration_status(grids: int):
    locked = state.get("sunday_lock") or state.get("man_lock")
    if registration_end_passed():
        return "🔒", "Anmeldeschluss erreicht"
    if locked:
        return "🔒", "Grid gesperrt"
    drivers = len(state.get("drivers", []))
    capacity = grid_capacity(grids)
    if drivers >= capacity and grids >= MAX_GRIDS:
        return "🔴", "Alle Grids voll"
    return "🟢", "Anmeldung offen"


async def _refresh_chan_log(session: aiohttp.ClientSession) -> None:
    grids = int(state.get("last_grid_count", 0))
    log_content = read_discord_log()
    _, status_label = _registration_status(grids)
    title = state.get("event_title", "–")
    ev_dt = state.get("event_datetime", "–")
    payload = {
        "embeds": [
            {
                "title": "Stand",
                "description": f"**{title}** · {ev_dt}\nFahrer: {len(state.get('drivers', []))} · Grids: {grids} · {status_label}",
                "color": 0x3498db,
            },
            {
                "title": "Log",
                "description": f"```\n{log_content[-3800:] if log_content else '–'}\n```",
                "color": 0x2c2f33,
            },
        ]
    }
    await post_or_update_log(session, payload)


async def post_or_update_log(session: aiohttp.ClientSession, payload: dict) -> None:
    messages = await get_channel_messages(session, CHAN_LOG, DISCORD_TOKEN)
    log_msgs = [
        m for m in messages
        if m.get("author", {}).get("bot") is True
        and len(m.get("embeds", [])) == 2
    ]
    if log_msgs:
        if len(log_msgs) > 1:
            for extra in sorted(log_msgs, key=lambda m: int(m["id"]))[1:]:
                await discord_delete(session, f"/channels/{CHAN_LOG}/messages/{extra['id']}", DISCORD_TOKEN)
        target = min(log_msgs, key=lambda m: int(m["id"]))
        result = await discord_patch(session, f"/channels/{CHAN_LOG}/messages/{target['id']}", DISCORD_TOKEN, payload)
        if result:
            state["log_id"] = target["id"]
            save_state()
            return
    msg = await discord_post(session, f"/channels/{CHAN_LOG}/messages", DISCORD_TOKEN, payload)
    if msg:
        state["log_id"] = msg["id"]
        save_state()


# ─────────────────────────────────────────────
# Google Sheets Sync
# ─────────────────────────────────────────────
async def sync_to_sheets(session: aiohttp.ClientSession, event_type: str) -> None:
    if not GOOGLE_SHEETS_ID:
        log.warning("GOOGLE_SHEETS_ID nicht gesetzt – Sheet-Sync übersprungen.")
        return

    grids   = int(state.get("last_grid_count", 0))
    drivers = list(state.get("drivers", []))
    now_str = datetime.now(BERLIN).strftime("%d.%m.%Y %H:%M")

    def _do_sync():
        gc = _get_gspread_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)
        ws = sh.worksheet("Apollo-Grabber")
        ws.update(range_name="B1", values=[[f"Letzte Änderung:\n{now_str} Uhr"]], value_input_option="USER_ENTERED")
        ws.update(range_name="D1", values=[[grids]], value_input_option="USER_ENTERED")
        ws.update(range_name="Q1", values=[["\n".join(drivers)]], value_input_option="USER_ENTERED")
        ws.update(range_name="F1", values=[[0]], value_input_option="USER_ENTERED")
        log.info(f"Google Sheets aktualisiert (type={event_type}).")
        if event_type == "cleancodes":
            lc = sh.worksheet("LobbyCodes")
            lc.batch_clear(["A2:C500"])
            log.info("LobbyCodes geleert.")

    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, _do_sync)
        state["last_sync_sheets"] = iso_now()
    except Exception as e:
        log.error(f"Google Sheets Sync Fehler: {e}")


# ─────────────────────────────────────────────
# Lobby Code Cleanup
# ─────────────────────────────────────────────
async def clean_lobby_codes(session: aiohttp.ClientSession) -> None:
    log.info("Lobby-Code Bereinigung gestartet.")
    await delete_all_messages(session, CHAN_CODES, DISCORD_TOKEN_LOBBYCODEGRABBER)
    await discord_post(
        session,
        f"/channels/{CHAN_CODES}/messages",
        DISCORD_TOKEN_LOBBYCODEGRABBER,
        {"content": MSG_LOBBYCODES},
    )
    log.info("Lobby-Code Bereinigung abgeschlossen.")


async def clear_chan_log(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    await delete_all_bot_messages(session, CHAN_LOG, DISCORD_TOKEN, bot_user_id)
    state["log_id"] = ""
    save_state()
    log.info("CHAN_LOG bereinigt.")


# ─────────────────────────────────────────────
# News message senders (aus Original übernommen)
# ─────────────────────────────────────────────
import random

def _pick_bilingual(de_var: str, en_var: str) -> tuple[str, str]:
    de_opts = [s.strip() for s in de_var.split(";") if s.strip()] if de_var else [""]
    en_opts = [s.strip() for s in en_var.split(";") if s.strip()] if en_var else [""]
    idx    = random.randint(0, len(de_opts) - 1) if de_opts else 0
    en_idx = min(idx, len(en_opts) - 1) if en_opts else 0
    return (de_opts[idx] if de_opts else ""), (en_opts[en_idx] if en_opts else "")


def _format_bilingual(de_text: str, en_text: str) -> str:
    parts = []
    if de_text:
        parts.append(f"🇩🇪 {de_text}" if ENABLE_MULTILANGUAGE else de_text)
    if en_text and ENABLE_MULTILANGUAGE:
        parts.append(f"🇬🇧 {en_text}")
    return "\n".join(parts)


def _format_names(names: list, conjunction: str = "und") -> str:
    clean = [n.replace("\\", "") for n in names]
    if len(clean) <= 1:
        return clean[0] if clean else ""
    return ", ".join(clean[:-1]) + f" {conjunction} " + clean[-1]


async def send_sunday_msg(session: aiohttp.ClientSession) -> None:
    if not int(cfg("ENABLE_SUNDAY_MSG")):
        return
    if state.get("sunday_msg_sent"):
        return
    if not MSG_SUNDAY_TEXT:
        return
    de, en = _pick_bilingual(MSG_SUNDAY_TEXT, MSG_SUNDAY_TEXT_EN)
    text = _format_bilingual(de, en)
    if not text:
        return
    await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})
    state["sunday_msg_sent"] = True
    save_state()


async def send_waitlist_msg(session: aiohttp.ClientSession, waitlisted: list) -> None:
    if not int(cfg("ENABLE_WAITLIST_MSG")):
        return
    min_grids = int(cfg("SET_MIN_GRIDS_MSG"))
    if int(state.get("last_grid_count", 0)) < min_grids:
        return
    names_str = _format_names(waitlisted)
    mentions  = " ".join(_mention(n) for n in waitlisted)
    if len(waitlisted) == 1:
        de, en = _pick_bilingual(MSG_WAITLIST_SINGLE, MSG_WAITLIST_SINGLE_EN)
    else:
        de, en = _pick_bilingual(MSG_WAITLIST_MULTI, MSG_WAITLIST_MULTI_EN)
    de = de.replace("{name}", names_str).replace("{mention}", mentions)
    en = en.replace("{name}", names_str).replace("{mention}", mentions)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})


async def send_moved_up_msg(session: aiohttp.ClientSession, moved_up: list, removed: list) -> None:
    if not int(cfg("ENABLE_MOVED_UP_MSG")):
        return
    min_grids = int(cfg("SET_MIN_GRIDS_MSG"))
    if int(state.get("last_grid_count", 0)) < min_grids:
        return
    names_str = _format_names(moved_up)
    mentions  = " ".join(_mention(n) for n in moved_up)
    if len(moved_up) == 1:
        de, en = _pick_bilingual(MSG_MOVED_UP_SINGLE, MSG_MOVED_UP_SINGLE_EN)
    else:
        de, en = _pick_bilingual(MSG_MOVED_UP_MULTI, MSG_MOVED_UP_MULTI_EN)
    de = de.replace("{name}", names_str).replace("{mention}", mentions)
    en = en.replace("{name}", names_str).replace("{mention}", mentions)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})


async def send_grid_full_msg(session: aiohttp.ClientSession, new_grids: int) -> None:
    if not int(cfg("ENABLE_GRID_FULL_MSG")):
        return
    min_grids = int(cfg("SET_MIN_GRIDS_MSG"))
    if new_grids < min_grids:
        return
    de, en = _pick_bilingual(MSG_GRID_FULL_TEXT, MSG_GRID_FULL_TEXT_EN)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})


async def send_extra_grid_msg(session: aiohttp.ClientSession) -> None:
    if not int(cfg("ENABLE_EXTRA_GRID_MSG")):
        return
    de, en = _pick_bilingual(MSG_EXTRA_GRID_TEXT, MSG_EXTRA_GRID_TEXT_EN)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})


async def send_new_event_msg(session: aiohttp.ClientSession) -> None:
    if not int(cfg("SET_NEW_EVENT_MSG")):
        return
    de, en = _pick_bilingual(MSG_NEW_EVENT, MSG_NEW_EVENT_EN)
    text = _format_bilingual(de, en)
    if text:
        await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})


# ─────────────────────────────────────────────
# Grid-Change Notifications (Montag 18-20:30)
# ─────────────────────────────────────────────
def _read_grids_sheet() -> dict:
    """Liest aktuellen Grid-Stand aus dem Grids-Sheet. Gibt {psn: grid_name} zurück."""
    if not GOOGLE_SHEETS_ID:
        return {}
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(GOOGLE_SHEETS_ID)
        ws = sh.worksheet("Grids")
        all_data = ws.get_all_values()
        result = {}
        grid_col_offsets = [1, 6, 11, 16]
        for driver_col in grid_col_offsets:
            grid_name = None
            for row in all_data:
                if len(row) <= driver_col:
                    continue
                cell = row[driver_col].strip()
                if "Grid-" in cell or "Warteliste" in cell:
                    grid_name = cell
                    continue
                if cell and grid_name and row[driver_col + 1].strip() not in ("Host", "Streamer", ""):
                    result[cell] = grid_name
        return result
    except Exception as e:
        log.error(f"_read_grids_sheet Fehler: {e}")
        return {}


async def check_and_notify_grid_changes(
    session: aiohttp.ClientSession,
    added_this_cycle: list,
    grids_before: dict,
) -> None:
    if not MSG_GRID_CHANGE_TEXT:
        return
    loop = asyncio.get_event_loop()
    grids_after = await loop.run_in_executor(None, _read_grids_sheet)
    changed = []
    for driver, grid_after in grids_after.items():
        if driver in added_this_cycle:
            continue
        grid_before = grids_before.get(driver)
        if grid_before and grid_before != grid_after:
            changed.append((driver, grid_before, grid_after))
    for driver, before, after in changed:
        mention = _mention(driver)
        de, en = _pick_bilingual(MSG_GRID_CHANGE_TEXT, MSG_GRID_CHANGE_TEXT_EN)
        de = de.replace("{name}", driver).replace("{mention}", mention).replace("{before}", before).replace("{after}", after)
        en = en.replace("{name}", driver).replace("{mention}", mention).replace("{before}", before).replace("{after}", after)
        text = _format_bilingual(de, en)
        if text:
            await discord_post(session, f"/channels/{CHAN_NEWS}/messages", DISCORD_TOKEN, {"content": text})


# ─────────────────────────────────────────────
# HTML Dashboard
# ─────────────────────────────────────────────
def build_html_dashboard(grids: int) -> str:
    title        = state.get("event_title", "–")
    ev_dt        = state.get("event_datetime", "–")
    driver_count = len(state.get("drivers", []))
    locked       = state.get("sunday_lock") or state.get("man_lock")
    lock_symbol  = " 🔒" if locked else ""
    actual_grids = int(state.get("last_grid_count", grids))
    status_emoji, status_label = _registration_status(actual_grids)
    discord_log = (
        read_discord_log()
        .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )
    return f"""<!DOCTYPE html>
<html lang="de">
<head><meta charset="UTF-8"><title>RTC CheckinBot</title>
<style>
  body {{ background: #111; color: #eee; font-family: Arial, sans-serif; padding: 24px; max-width: 860px; margin: 0 auto; }}
  h1 {{ color: #7ec8e3; margin-bottom: 4px; }}
  .status {{ font-size: 1.1em; margin-bottom: 6px; }}
  .event-title {{ font-size: 1.3em; font-weight: bold; margin-bottom: 2px; }}
  .event-dt {{ color: #aaa; margin-bottom: 12px; }}
  .info {{ color: #ccc; margin-bottom: 16px; }}
  .log {{ background: #000; color: #0f0; font-family: monospace; padding: 15px; white-space: pre-wrap; border-radius: 6px; }}
  a {{ color: #7ec8e3; }}
</style>
</head>
<body>
<h1>RTC CheckinBot{'  [TESTMODUS]' if TESTMODUS else ''}</h1>
<div class="status">{status_emoji} {status_label}</div>
<div class="event-title">{title}</div>
<div class="event-dt">{ev_dt}</div>
<div class="info">Fahrer: {driver_count} &nbsp;|&nbsp; Grids: {actual_grids}{lock_symbol}</div>
<div class="info"><a href="https://cutt.ly/RTC-infos" target="_blank">https://cutt.ly/RTC-infos</a></div>
<div class="log">{discord_log}</div>
</body>
</html>"""


# ─────────────────────────────────────────────
# Command validation
# ─────────────────────────────────────────────
def _validate_var(param: str, val: str) -> str | None:
    if param in {
        "ENABLE_EXTRA_GRID", "ENABLE_MOVED_UP_MSG", "ENABLE_NEWS_CLEANUP",
        "ENABLE_SUNDAY_MSG", "ENABLE_WAITLIST_MSG", "ENABLE_EXTRA_GRID_MSG",
        "ENABLE_GRID_FULL_MSG", "SET_MSG_MOVED_UP_TEXT", "SET_NEW_EVENT_MSG",
    }:
        if val not in ("0", "1"):
            return f"{param} muss 0 oder 1 sein."
        return None
    if param == "EXTRA_GRID_THRESHOLD":
        try:
            if not (1 <= int(val) <= 10):
                return "EXTRA_GRID_THRESHOLD muss zwischen 1 und 10 liegen."
        except ValueError:
            return "EXTRA_GRID_THRESHOLD muss eine ganze Zahl sein."
        return None
    if param == "REGISTRATION_END_TIME":
        if not re.match(r"^\d{2}:\d{2}$", val):
            return "REGISTRATION_END_TIME muss im Format hh:mm sein."
        return None
    if param == "POLL_INTERVAL_SECONDS":
        try:
            v = int(val)
            if not (10 <= v <= 120):
                return "POLL_INTERVAL_SECONDS muss zwischen 10 und 120 liegen."
        except ValueError:
            return "POLL_INTERVAL_SECONDS muss eine ganze Zahl sein."
        return None
    if param == "SET_MIN_GRIDS_MSG":
        try:
            v = int(val)
            if not (1 <= v <= MAX_GRIDS):
                return f"SET_MIN_GRIDS_MSG muss zwischen 1 und {MAX_GRIDS} liegen."
        except ValueError:
            return "SET_MIN_GRIDS_MSG muss eine ganze Zahl sein."
        return None
    return None


# ─────────────────────────────────────────────
# Command handler
# ─────────────────────────────────────────────
KNOWN_COMMANDS = ("!help", "!clean", "!set", "!sync", "!grids=")


def _is_command(content: str) -> bool:
    lower = content.lower().strip()
    return any(lower == cmd or lower.startswith(cmd) for cmd in KNOWN_COMMANDS)


async def handle_commands(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    messages = await get_channel_messages(session, CHAN_ORDERS, DISCORD_TOKEN, limit=20)
    for msg in messages:
        author_id = msg.get("author", {}).get("id", "")
        content   = msg.get("content", "").strip()
        msg_id    = msg.get("id", "")
        username  = await get_display_name(session, author_id, msg.get("author", {}).get("username", "Unknown"))

        if not _is_command(content):
            continue

        await discord_delete(session, f"/channels/{CHAN_ORDERS}/messages/{msg_id}", DISCORD_TOKEN)

        if author_id not in USER_ID_ORGA:
            continue

        content_lower = content.lower()
        ts = ts_str()
        grids = int(state.get("last_grid_count", 0))

        if content_lower == "!help":
            await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN, {"content": MSG_HILFETEXT})
            continue

        if content_lower == "!clean":
            await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN, {"content": (
                "**!clean** – Verfügbare Optionen:\n"
                "`!clean codes` – Lobby-Code-Kanal leeren\n"
                "`!clean log` – Log neu aufbauen\n"
                "`!clean news` – Alle Bot-Nachrichten im News-Kanal löschen"
            )})
            continue

        if content_lower == "!clean codes":
            await clean_lobby_codes(session)
            await sync_to_sheets(session, "cleancodes")
            append_event_log(f"{ts} ⚙️ Lobby-Bereinigung durch {username}")
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        if content_lower == "!clean log":
            clean_content = build_clean_log(grids, username)
            EVENT_LOG_FILE.write_text(clean_content, encoding="utf-8")
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        if content_lower == "!clean news":
            await delete_all_bot_messages(session, CHAN_NEWS, DISCORD_TOKEN, bot_user_id)
            append_event_log(f"{ts} ⚙️ News-Bereinigung durch {username}")
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            continue

        if content_lower == "!sync":
            await sync_to_sheets(session, "update")
            save_state()
            append_event_log(f"{ts} ⚙️ Manueller Sheets-Sync durch {username}")
            _rebuild_discord_log(grids)
            await _refresh_chan_log(session)
            await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN, {"content": "✅ Google Sheets Sync ausgelöst."})
            continue

        if content_lower.startswith("!set"):
            parts = content.split(maxsplit=2)
            if len(parts) == 1:
                lines = []
                for vk in VAR_KEYS:
                    val = state.get(vk, DEFAULT_STATE.get(vk, "–"))
                    lines.append(f"`{vk[4:]}` = `{val}`")
                await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN,
                    {"content": "**Aktuelle Einstellungen:**", "embeds": [{"description": "\n".join(lines)}]})
            elif len(parts) == 2:
                await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN,
                    {"content": f"❌ Kein Wert angegeben. Verwendung: `!set {parts[1].upper()} <Wert>`"})
            else:
                param   = parts[1].upper()
                val_raw = parts[2].strip()
                var_key = f"var_{param}"
                if var_key not in VAR_KEYS:
                    await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN,
                        {"content": f"❌ Unbekannter Parameter: `{param}`"})
                    continue
                err = _validate_var(param, val_raw)
                if err:
                    await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN,
                        {"content": f"❌ Ungültiger Wert: {err}"})
                else:
                    state[var_key] = _coerce_var(var_key, val_raw)
                    save_state()
                    append_event_log(f"{ts} ⚠️ {param} geändert durch {username}: {val_raw}")
                    _rebuild_discord_log(grids)
                    await _refresh_chan_log(session)
                    await discord_post(session, f"/channels/{CHAN_ORDERS}/messages", DISCORD_TOKEN,
                        {"content": f"✅ `{param}` gesetzt auf `{val_raw}`"})
            continue

        m = re.match(r"!grids=(\d+)", content_lower)
        if m:
            x = int(m.group(1))
            if x == 0:
                state["man_lock"] = False
                state["manual_grids"] = None
                state["grid_lock_override"] = True
                new_g = recalculate_grids(len(state.get("drivers", [])))
                state["last_grid_count"] = new_g
            else:
                new_g = min(x, MAX_GRIDS)
                state["man_lock"]     = True
                state["manual_grids"] = new_g
                state["last_grid_count"] = new_g

            current_drivers = list(state.get("drivers", []))
            old_status      = dict(state.get("driver_status", {}))
            new_status      = classify_drivers(current_drivers, new_g)
            state["driver_status"] = new_status

            newly_waitlisted = [n for n, s in new_status.items() if s == "waitlist" and old_status.get(n) == "grid"]
            newly_moved_up   = [n for n, s in new_status.items() if s == "grid" and old_status.get(n) == "waitlist"]

            append_event_log(f"{ts} {'🔓 Grid-Automatik reaktiviert' if x == 0 else f'🔒 Grids auf {new_g} gesetzt'} durch {username}")
            for n in newly_moved_up:
                append_event_log(f"{ts} 🟡 -> 🟢 {n}")
            for n in newly_waitlisted:
                append_event_log(f"{ts} 🟢 -> 🟡 {n}")

            save_state()
            _rebuild_discord_log(new_g)
            await _refresh_chan_log(session)
            await update_checkin_message(session)

            if newly_waitlisted:
                await send_waitlist_msg(session, newly_waitlisted)
            if newly_moved_up:
                await send_moved_up_msg(session, newly_moved_up, [])
            continue


# ─────────────────────────────────────────────
# Pipeline mutex
# ─────────────────────────────────────────────
_pipeline_lock  = asyncio.Lock()
_tuesday_reset_done = False  # Verhindert doppeltes Auslösen am Dienstag


# ─────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────
async def bootstrap(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    log.info("Bootstrap: Erstinitialisierung …")
    for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE):
        fp.write_text("", encoding="utf-8")
    state["checkin_message_id"] = ""
    state["log_id"] = ""
    state["registration_end_monday"] = ""
    if is_sunday_lock_time():
        state["sunday_lock"]     = True
        state["sunday_msg_sent"] = True
    save_state()
    append_event_log(f"{ts_str()} ⚙️ Systemupdate")
    await refresh_driver_discord_map(session)
    log.info("Bootstrap abgeschlossen.")


# ─────────────────────────────────────────────
# Tuesday Reset – Kernfunktion des neuen Bots
# ─────────────────────────────────────────────
async def tuesday_reset(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    """
    Dienstags um 09:00 Berliner Zeit:
    1. Kalender-DB abfragen
    2. CHAN_CHECKIN komplett leeren
    3. Dauerangemeldete laden + als Fahrerliste eintragen
    4. Checkin-Nachricht mit Buttons posten
    5. Sofort Google Sheets syncen
    6. CHAN_CODES und CHAN_LOG bereinigen
    7. News-Kanal bereinigen (wenn aktiv)
    """
    log.info("Dienstags-Reset gestartet.")
    had_previous_event = bool(state.get("event_title") or state.get("drivers"))

    # ── 1. Nächstes Rennen aus DB ──────────────────────────────────────────
    race = get_next_race()
    is_pause = (race is None) or bool(race.get("is_pause"))

    # ── 2. State zurücksetzen ──────────────────────────────────────────────
    state["new_event"]               = 1
    state["sunday_lock"]             = False
    state["man_lock"]                = False
    state["manual_grids"]            = None
    state["registration_end_logged"] = False
    state["registration_end_locked"] = False
    state["ignored_drivers"]         = []
    state["driver_status"]           = {}
    state["last_grid_count"]         = 0
    state["sunday_msg_sent"]         = False
    state["registration_end_monday"] = ""
    state["checkin_message_id"]      = ""

    if not is_pause and race:
        state["event_title"]    = f"Rennen {race['race_number']} – {race['track_name']}"
        state["event_datetime"] = f"{race['race_date']} 20:45"
    else:
        state["event_title"]    = "Pause"
        state["event_datetime"] = ""

    set_registration_end_monday()

    # Log-Dateien leeren
    for fp in (EVENT_LOG_FILE, DISCORD_LOG_FILE, ANMELDUNGEN_FILE):
        fp.write_text("", encoding="utf-8")
    append_event_log(f"{ts_str()} ⚙️ New Event")

    # ── 3. Dauerangemeldete laden ──────────────────────────────────────────
    if not is_pause:
        abo_drivers = read_abo()
        if abo_drivers:
            log.info(f"Dauerangemeldete: {abo_drivers}")
            state["drivers"] = list(abo_drivers)
            new_grids = recalculate_grids(len(abo_drivers))
            state["driver_status"] = classify_drivers(abo_drivers, new_grids)
            write_anmeldungen(abo_drivers)
            for name in abo_drivers:
                append_event_log(f"{ts_str()} 🟢 {name} (Abo)")
        else:
            state["drivers"] = []
    else:
        state["drivers"] = []

    save_state()

    # ── 4. CHAN_CHECKIN leeren + neue Nachricht posten ─────────────────────
    await post_checkin_message(session, race)

    # ── 5. Google Sheets sofort syncen ─────────────────────────────────────
    await sync_to_sheets(session, "cleancodes" if had_previous_event else "update")

    # ── 6. Lobby Codes + CHAN_LOG bereinigen ───────────────────────────────
    if had_previous_event:
        await clean_lobby_codes(session)
        await clear_chan_log(session, bot_user_id)
        await asyncio.sleep(2)
        if int(cfg("ENABLE_NEWS_CLEANUP")):
            await delete_all_bot_messages(session, CHAN_NEWS, DISCORD_TOKEN, bot_user_id)

    # ── 7. Discord-Map auffrischen ─────────────────────────────────────────
    await refresh_driver_discord_map(session)

    # ── 8. CHAN_LOG neu aufbauen ───────────────────────────────────────────
    _rebuild_discord_log(int(state.get("last_grid_count", 0)))
    await _refresh_chan_log(session)

    if had_previous_event:
        await send_new_event_msg(session)

    state["new_event"] = 0
    save_state()
    log.info("Dienstags-Reset abgeschlossen.")


# ─────────────────────────────────────────────
# Core pipeline
# ─────────────────────────────────────────────
async def run_pipeline(session: aiohttp.ClientSession, bot_user_id: str) -> None:
    global _tuesday_reset_done

    # ── 0. Sunday-lock ─────────────────────────────────────────────────────
    if is_sunday_lock_time() and not state.get("sunday_lock"):
        state["sunday_lock"] = True
        log.info("Sonntags-Sperre aktiviert.")
        save_state()

    # ── 1. Dienstags-Reset ─────────────────────────────────────────────────
    if is_tuesday_reset_time():
        if not _tuesday_reset_done:
            _tuesday_reset_done = True
            async with _pipeline_lock:
                await tuesday_reset(session, bot_user_id)
        return
    else:
        # Außerhalb des Reset-Fensters: Flag zurücksetzen für nächste Woche
        _tuesday_reset_done = False

    # ── 2. Commands abarbeiten ─────────────────────────────────────────────
    await handle_commands(session, bot_user_id)

    # ── 3. Sunday-lock Nachricht ───────────────────────────────────────────
    if is_sunday_lock_time():
        await send_sunday_msg(session)

    # ── 4. Anmeldeschluss prüfen ───────────────────────────────────────────
    if registration_end_passed() and not state.get("registration_end_logged"):
        append_event_log(f"{ts_str()} ⚙️ Anmeldeschluss erreicht")
        state["registration_end_logged"] = True
        state["registration_end_locked"] = True
        save_state()
        _rebuild_discord_log(int(state.get("last_grid_count", 0)))
        await _refresh_chan_log(session)

    # ── 5. Monday Grid-Change Notifications ───────────────────────────────
    if _is_monday_gridchange_time() and state.get("drivers"):
        loop = asyncio.get_event_loop()
        grids_before = await loop.run_in_executor(None, _read_grids_sheet)
        if grids_before:
            await check_and_notify_grid_changes(session, [], grids_before)


# ─────────────────────────────────────────────
# Gateway / Interaction Listener
# ─────────────────────────────────────────────
# HINWEIS: Discord Buttons benötigen entweder:
# (a) Discord Gateway (WebSocket) – empfohlen für vollständige Bot-Funktionalität
# (b) Interaction Endpoint URL (Webhook) – für einfachere Deployments
#
# Da dieser Bot bisher REST-only (aiohttp) arbeitet, wird für Buttons
# ein einfacher Interaction-Webhook via Flask integriert.
# Der Flask-Endpunkt /interactions empfängt POST-Requests von Discord
# und leitet sie an handle_interaction weiter.

import hmac
import hashlib

_interaction_queue: asyncio.Queue = asyncio.Queue()


def _verify_discord_signature(body: bytes, signature: str, timestamp: str) -> bool:
    """Verifiziert die Discord-Signatur für Interaction-Webhooks."""
    public_key = _env("DISCORD_PUBLIC_KEY", "")
    if not public_key:
        return True  # Keine Verifikation wenn kein Key gesetzt
    try:
        from nacl.signing import VerifyKey
        from nacl.exceptions import BadSignatureError
        vk = VerifyKey(bytes.fromhex(public_key))
        vk.verify((timestamp + body.decode()).encode(), bytes.fromhex(signature))
        return True
    except Exception:
        return False


# Flask-Route für Discord Interactions
from flask import request as flask_request

@flask_app.route("/interactions", methods=["POST"])
def interactions_endpoint():
    signature = flask_request.headers.get("X-Signature-Ed25519", "")
    timestamp  = flask_request.headers.get("X-Signature-Timestamp", "")
    body       = flask_request.get_data()

    if not _verify_discord_signature(body, signature, timestamp):
        return "Invalid signature", 401

    data = json.loads(body)

    # Ping von Discord beantworten
    if data.get("type") == 1:
        return json.dumps({"type": 1}), 200, {"Content-Type": "application/json"}

    # Interaction in die Queue einreihen (asyncio verarbeitet sie im Worker)
    try:
        _interaction_queue.put_nowait(data)
    except asyncio.QueueFull:
        log.warning("Interaction-Queue voll!")

    # Sofort mit "deferred" antworten (Discord erwartet Antwort innerhalb 3s)
    return json.dumps({"type": 5, "data": {"flags": 64}}), 200, {"Content-Type": "application/json"}


# ─────────────────────────────────────────────
# Worker loop
# ─────────────────────────────────────────────
async def worker(fresh_install: bool) -> None:
    log.info("Worker gestartet.")
    ensure_files()

    async with aiohttp.ClientSession() as session:
        bot_user_id = await get_bot_user_id(session, DISCORD_TOKEN)

        if fresh_install:
            await bootstrap(session, bot_user_id)

        while True:
            # ── Interactions aus Queue verarbeiten ────────────────────────
            while not _interaction_queue.empty():
                try:
                    interaction = _interaction_queue.get_nowait()
                    await handle_interaction(session, interaction)
                except Exception as e:
                    log.error(f"Interaction-Fehler: {e}", exc_info=True)

            # ── Pipeline ──────────────────────────────────────────────────
            async with _pipeline_lock:
                try:
                    await run_pipeline(session, bot_user_id)
                except Exception as e:
                    log.error(f"Pipeline-Fehler: {e}", exc_info=True)
                    save_state()

            # ── Inter-pipeline Command Scan ───────────────────────────────
            poll_interval = int(state.get("var_POLL_INTERVAL_SECONDS", 60))
            cmd_interval  = max(1, CMD_SCAN_INTERVAL_SECONDS)
            elapsed = 0

            while elapsed < poll_interval:
                sleep_for = min(cmd_interval, poll_interval - elapsed)
                await asyncio.sleep(sleep_for)
                elapsed += sleep_for

                # Interactions auch zwischen Zyklen verarbeiten
                while not _interaction_queue.empty():
                    try:
                        interaction = _interaction_queue.get_nowait()
                        await handle_interaction(session, interaction)
                    except Exception as e:
                        log.error(f"Interaction-Fehler (mid-cycle): {e}", exc_info=True)

                if elapsed < poll_interval:
                    try:
                        await handle_commands(session, bot_user_id)
                    except Exception as e:
                        log.warning(f"Command-Scan Fehler: {e}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    fresh_install = not STATE_FILE.exists()
    load_state()
    ensure_files()
    launch_flask_thread()
    try:
        asyncio.run(worker(fresh_install))
    except KeyboardInterrupt:
        log.info("Shutdown durch KeyboardInterrupt.")
        save_state()
