"""
RTC CheckinBot – checkin_bot.py
Hauptbot: Discord Interactions, Buttons, Wochenlogik, Scheduler
"""

import asyncio
import json
import logging
import os
import random
import threading
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

import aiohttp
from flask import Flask, request, jsonify
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError
from dotenv import load_dotenv

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

try:
    _fh = RotatingFileHandler(
        "checkin_bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(_fh)
except Exception:
    pass

BERLIN = ZoneInfo("Europe/Berlin")

# ─────────────────────────────────────────────
# Env-Helfer
# ─────────────────────────────────────────────

def _env(key, default=None):
    return os.environ.get(key, default)

def _env_int(key, default):
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

def _env_msg(key, default=""):
    val = os.environ.get(key, default)
    return val.replace("\\n", "\n") if val else default

def _pick_msg(raw: str, **kwargs) -> str:
    """Wählt zufällig eine Variante aus einem semikolon-getrennten Text und ersetzt Platzhalter."""
    if not raw:
        return ""
    variants = [v.strip() for v in raw.split(";") if v.strip()]
    text = random.choice(variants) if variants else raw
    for k, v in kwargs.items():
        text = text.replace("{" + k + "}", str(v))
    return text

# ─────────────────────────────────────────────
# Konfiguration
# ─────────────────────────────────────────────
DISCORD_TOKEN_CHECKINBOT      = _env("DISCORD_TOKEN_CHECKINBOT", "")
DISCORD_TOKEN_LOBBYCODEGRABBER = _env("DISCORD_TOKEN_LOBBYCODEGRABBER", "")
DISCORD_PUBLIC_KEY            = _env("DISCORD_PUBLIC_KEY", "")
DISCORD_GUILD_ID              = _env("DISCORD_GUILD_ID", "")
CHAN_CHECKIN                  = _env("CHAN_CHECKIN", "")
CHAN_CHECKIN_MSG_ID            = _env("CHAN_CHECKIN_MSG_ID", "")
CHAN_NEWS                     = _env("CHAN_NEWS", "")
CHAN_CODES                    = _env("CHAN_CODES", "")
CHAN_ORGA                     = _env("CHAN_ORGA", "")
USER_ID_ORGA                  = [u.strip() for u in _env("USER_ID_ORGA", "").split(";") if u.strip()]
DRIVERS_PER_GRID              = _env_int("DRIVERS_PER_GRID", 15)
MAX_GRIDS                     = _env_int("MAX_GRIDS", 4)
REGISTRATION_DEADLINE         = _env("REGISTRATION_DEADLINE", "20:45")
LOBBY_OPEN                    = _env("LOBBY_OPEN", "20:30")
TEST_MODE                     = _env("TEST_MODE", "false").lower() == "true"
PORT                          = _env_int("PORT", 8081)

ENABLE_EXTRA_GRID    = _env_int("ENABLE_EXTRA_GRID", 0)
EXTRA_GRID_THRESHOLD = _env_int("EXTRA_GRID_THRESHOLD", 10)
ENABLE_EXTRA_GRID_MSG = _env_int("ENABLE_EXTRA_GRID_MSG", 1)
ENABLE_MOVED_UP_MSG  = _env_int("ENABLE_MOVED_UP_MSG", 1)
ENABLE_WAITLIST_MSG  = _env_int("ENABLE_WAITLIST_MSG", 1)
ENABLE_SUNDAY_MSG    = _env_int("ENABLE_SUNDAY_MSG", 1)
ENABLE_GRID_FULL_MSG = _env_int("ENABLE_GRID_FULL_MSG", 1)
ENABLE_MULTILANGUAGE = _env_int("ENABLE_MULTILANGUAGE", 0)
SET_MIN_GRIDS_MSG    = _env_int("SET_MIN_GRIDS_MSG", 3)

MSG_EXTRA_GRID_TEXT    = _env_msg("MSG_EXTRA_GRID_TEXT")
MSG_EXTRA_GRID_TEXT_EN = _env_msg("MSG_EXTRA_GRID_TEXT_EN")
MSG_GRID_FULL_TEXT     = _env_msg("MSG_GRID_FULL_TEXT")
MSG_GRID_FULL_TEXT_EN  = _env_msg("MSG_GRID_FULL_TEXT_EN")
MSG_MOVED_UP_SINGLE    = _env_msg("MSG_MOVED_UP_SINGLE")
MSG_MOVED_UP_SINGLE_EN = _env_msg("MSG_MOVED_UP_SINGLE_EN")
MSG_MOVED_UP_MULTI     = _env_msg("MSG_MOVED_UP_MULTI")
MSG_MOVED_UP_MULTI_EN  = _env_msg("MSG_MOVED_UP_MULTI_EN")
MSG_SUNDAY_TEXT        = _env_msg("MSG_SUNDAY_TEXT")
MSG_SUNDAY_TEXT_EN     = _env_msg("MSG_SUNDAY_TEXT_EN")
MSG_WAITLIST_SINGLE    = _env_msg("MSG_WAITLIST_SINGLE")
MSG_WAITLIST_SINGLE_EN = _env_msg("MSG_WAITLIST_SINGLE_EN")
MSG_WAITLIST_MULTI     = _env_msg("MSG_WAITLIST_MULTI")
MSG_WAITLIST_MULTI_EN  = _env_msg("MSG_WAITLIST_MULTI_EN")
MSG_NEW_EVENT          = _env_msg("MSG_NEW_EVENT")
MSG_NEW_EVENT_EN       = _env_msg("MSG_NEW_EVENT_EN")
MSG_NEW_EVENT_TEXT     = _env_msg("MSG_NEW_EVENT_TEXT")
MSG_NEW_EVENT_TEXT_EN  = _env_msg("MSG_NEW_EVENT_TEXT_EN")
MSG_LOBBYCODES         = _env_msg("MSG_LOBBYCODES")
MSG_HILFETEXT          = _env_msg("MSG_HILFETEXT")
MSG_GRID_CHANGE_TEXT   = _env_msg("MSG_GRID_CHANGE_TEXT")
MSG_GRID_CHANGE_TEXT_EN = _env_msg("MSG_GRID_CHANGE_TEXT_EN")

DISCORD_API = "https://discord.com/api/v10"

# ─────────────────────────────────────────────
# State (In-Memory)
# ─────────────────────────────────────────────
state = {
    "current_race_id": None,
    "current_race": None,
    "checkin_msg_id": CHAN_CHECKIN_MSG_ID or None,
    "sunday_lock": False,
    "sunday_msg_sent": False,
    "last_grid_count": 0,
    "grid_locked": False,
}

# ─────────────────────────────────────────────
# Discord API Helpers
# ─────────────────────────────────────────────

async def discord_request(session: aiohttp.ClientSession, method: str, endpoint: str,
                           token: str = None, **kwargs):
    """Sendet einen Request an die Discord API."""
    if token is None:
        token = DISCORD_TOKEN_CHECKINBOT
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }
    url = f"{DISCORD_API}{endpoint}"
    async with session.request(method, url, headers=headers, **kwargs) as resp:
        if resp.status in (200, 201):
            return await resp.json()
        elif resp.status == 204:
            return None
        else:
            text = await resp.text()
            log.error(f"Discord API {method} {endpoint} → {resp.status}: {text}")
            return None


async def send_message(session, channel_id: str, content: str,
                        components: list = None, token: str = None) -> dict | None:
    """Sendet eine neue Nachricht in einen Channel."""
    payload = {"content": content}
    if components:
        payload["components"] = components
    return await discord_request(
        session, "POST", f"/channels/{channel_id}/messages",
        token=token, json=payload
    )


async def edit_message(session, channel_id: str, message_id: str,
                        content: str, components: list = None, token: str = None) -> dict | None:
    """Bearbeitet eine bestehende Nachricht."""
    payload = {"content": content}
    if components is not None:
        payload["components"] = components
    return await discord_request(
        session, "PATCH", f"/channels/{channel_id}/messages/{message_id}",
        token=token, json=payload
    )


async def delete_message(session, channel_id: str, message_id: str, token: str = None):
    """Löscht eine Nachricht."""
    await discord_request(
        session, "DELETE", f"/channels/{channel_id}/messages/{message_id}",
        token=token
    )


async def bulk_delete_messages(session, channel_id: str, token: str = None):
    """Löscht alle Nachrichten in einem Channel (bulk delete, max 100)."""
    msgs = await discord_request(
        session, "GET", f"/channels/{channel_id}/messages?limit=100",
        token=token
    )
    if not msgs:
        return
    ids = [m["id"] for m in msgs]
    if len(ids) == 1:
        await delete_message(session, channel_id, ids[0], token=token)
    elif len(ids) > 1:
        await discord_request(
            session, "POST", f"/channels/{channel_id}/messages/bulk-delete",
            token=token, json={"messages": ids}
        )


async def get_guild_member(session, discord_id: str) -> dict | None:
    """Gibt den Guild-Member zurück."""
    return await discord_request(
        session, "GET",
        f"/guilds/{DISCORD_GUILD_ID}/members/{discord_id}"
    )


async def get_member_nickname(session, discord_id: str) -> str:
    """Gibt den Server-Nickname eines Members zurück."""
    member = await get_guild_member(session, discord_id)
    if not member:
        return ""
    return member.get("nick") or member.get("user", {}).get("username", "")


# ─────────────────────────────────────────────
# Effektiver Channel für Nachrichten
# ─────────────────────────────────────────────

def _news_channel() -> str:
    """Im Testmodus landen News im Checkin-Channel."""
    return CHAN_CHECKIN if TEST_MODE else CHAN_NEWS


def _codes_channel() -> str:
    """Im Testmodus landen Code-Nachrichten im Checkin-Channel."""
    return CHAN_CHECKIN if TEST_MODE else CHAN_CODES


# ─────────────────────────────────────────────
# Buttons
# ─────────────────────────────────────────────

def _build_buttons(show: bool) -> list:
    """Baut die Button-Komponenten. Bei show=False leere Liste."""
    if not show:
        return []
    return [
        {
            "type": 1,
            "components": [
                {"type": 2, "style": 3, "label": "Anmelden",       "custom_id": "checkin_register"},
                {"type": 2, "style": 4, "label": "Abmelden",       "custom_id": "checkin_unregister"},
                {"type": 2, "style": 1, "label": "Daueranmeldung", "custom_id": "checkin_abo_add"},
                {"type": 2, "style": 4, "label": "Dauerabmeldung", "custom_id": "checkin_abo_remove"},
                {"type": 2, "style": 2, "label": "Status",         "custom_id": "checkin_status"},
            ]
        }
    ]


# ─────────────────────────────────────────────
# Channel-Nachricht aktualisieren
# ─────────────────────────────────────────────

async def update_checkin_message(session):
    """Aktualisiert die Channel-Nachricht (oder postet sie neu wenn noch nicht vorhanden)."""
    from message_builder import build_channel_message
    from db import get_next_monday_race

    race = state.get("current_race")
    race_id = state.get("current_race_id")
    message, show_buttons = build_channel_message(race_id=race_id, race=race)
    components = _build_buttons(show_buttons)
    msg_id = state.get("checkin_msg_id")

    if msg_id:
        result = await edit_message(session, CHAN_CHECKIN, msg_id, message, components)
        if result is None:
            # Nachricht nicht mehr vorhanden – neu posten
            state["checkin_msg_id"] = None
            msg_id = None

    if not msg_id:
        result = await send_message(session, CHAN_CHECKIN, message, components)
        if result:
            new_id = result["id"]
            state["checkin_msg_id"] = new_id
            _save_msg_id(new_id)
            log.info(f"Neue Checkin-Nachricht gepostet: {new_id}")


def _save_msg_id(msg_id: str):
    """Speichert die Message-ID in der .env Datei."""
    try:
        env_path = ".env"
        with open(env_path, "r") as f:
            lines = f.readlines()
        with open(env_path, "w") as f:
            for line in lines:
                if line.startswith("CHAN_CHECKIN_MSG_ID="):
                    f.write(f"CHAN_CHECKIN_MSG_ID={msg_id}\n")
                else:
                    f.write(line)
        log.info(f"Message-ID {msg_id} in .env gespeichert.")
    except Exception as e:
        log.error(f"Fehler beim Speichern der Message-ID: {e}")


# ─────────────────────────────────────────────
# Orga-Benachrichtigung
# ─────────────────────────────────────────────

_orga_queue: list[str] = []

def queue_orga_message(text: str):
    """Fügt eine Orga-Nachricht zur Queue hinzu."""
    _orga_queue.append(text)

async def flush_orga_messages(session):
    """Sendet alle ausstehenden Orga-Nachrichten."""
    while _orga_queue:
        msg = _orga_queue.pop(0)
        await send_message(session, CHAN_ORGA, msg)


# ─────────────────────────────────────────────
# Grid-Logik
# ─────────────────────────────────────────────

def calculate_grids(driver_count: int) -> int:
    """Berechnet die Anzahl der Grids."""
    from db import get_grid_override
    race_id = state.get("current_race_id")
    if race_id:
        override = get_grid_override(race_id)
        if override:
            return override["grid_count"]

    if driver_count == 0:
        return 0
    grids = max(1, driver_count // DRIVERS_PER_GRID)

    # Extra-Grid Logik
    if ENABLE_EXTRA_GRID:
        remainder = driver_count % DRIVERS_PER_GRID
        if remainder >= EXTRA_GRID_THRESHOLD:
            grids += 1

    return min(grids, MAX_GRIDS)


async def send_grid_full_msg(session, new_grids: int):
    """Sendet Grid-Full Nachricht wenn neue Grids aufgehen."""
    if not ENABLE_GRID_FULL_MSG:
        return
    if new_grids < SET_MIN_GRIDS_MSG:
        return
    raw = MSG_GRID_FULL_TEXT_EN if ENABLE_MULTILANGUAGE else MSG_GRID_FULL_TEXT
    text = _pick_msg(raw, full_grids=new_grids)
    if text:
        await send_message(session, _news_channel(), text)


async def send_extra_grid_msg(session):
    """Sendet Extra-Grid Nachricht."""
    if not ENABLE_EXTRA_GRID_MSG:
        return
    raw = MSG_EXTRA_GRID_TEXT_EN if ENABLE_MULTILANGUAGE else MSG_EXTRA_GRID_TEXT
    text = _pick_msg(raw)
    if text:
        await send_message(session, _news_channel(), text)


async def send_waitlist_msg(session, driver_names: list):
    """Sendet Wartelisten-Nachricht."""
    if not ENABLE_WAITLIST_MSG:
        return
    names_str = ", ".join(driver_names)
    if len(driver_names) == 1:
        raw = MSG_WAITLIST_SINGLE_EN if ENABLE_MULTILANGUAGE else MSG_WAITLIST_SINGLE
    else:
        raw = MSG_WAITLIST_MULTI_EN if ENABLE_MULTILANGUAGE else MSG_WAITLIST_MULTI
    text = _pick_msg(raw, driver_names=names_str)
    if text:
        await send_message(session, _news_channel(), text)


async def send_moved_up_msg(session, driver_names: list):
    """Sendet Nachrücker-Nachricht."""
    if not ENABLE_MOVED_UP_MSG:
        return
    names_str = ", ".join(driver_names)
    if len(driver_names) == 1:
        raw = MSG_MOVED_UP_SINGLE_EN if ENABLE_MULTILANGUAGE else MSG_MOVED_UP_SINGLE
    else:
        raw = MSG_MOVED_UP_MULTI_EN if ENABLE_MULTILANGUAGE else MSG_MOVED_UP_MULTI
    text = _pick_msg(raw, driver_names=names_str)
    if text:
        await send_message(session, _news_channel(), text)


async def send_sunday_msg(session):
    """Sendet die Sonntags-18-Uhr Nachricht."""
    if not ENABLE_SUNDAY_MSG:
        return
    if state.get("sunday_msg_sent"):
        return
    race_id = state.get("current_race_id")
    if not race_id:
        return
    from db import get_registration_count
    driver_count = get_registration_count(race_id)
    grid_count = calculate_grids(driver_count)
    free_slots = max(0, grid_count * DRIVERS_PER_GRID - driver_count)
    raw = MSG_SUNDAY_TEXT_EN if ENABLE_MULTILANGUAGE else MSG_SUNDAY_TEXT
    text = _pick_msg(raw, grids=grid_count, driver_count=driver_count, free_slots=free_slots)
    if text:
        await send_message(session, _news_channel(), text)
    state["sunday_msg_sent"] = True
    state["grid_locked"] = True
    log.info(f"Sunday-Lock: {grid_count} Grids, {driver_count} Fahrer.")
    from db import save_state
    save_state({
        "sunday_msg_sent": True,
        "grid_locked": True,
        "last_grid_count": grid_count,
    })


# ─────────────────────────────────────────────
# Button-Handler
# ─────────────────────────────────────────────

async def handle_register(session, discord_id: str, nickname: str) -> str:
    """Verarbeitet den Anmelden-Button."""
    from db import (
        get_registration, add_registration, add_log_entry,
        get_registration_count
    )
    from driver_resolver import resolve_driver
    from sheets import sync_registrations_to_sheet

    race_id = state.get("current_race_id")
    if not race_id:
        return "❌ Kein aktives Rennen gefunden."

    driver = resolve_driver(discord_id, nickname, queue_orga_message)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden. Die Orga wurde informiert."

    driver_id = driver["driver_id"]

    # Bereits angemeldet?
    if get_registration(race_id, driver_id):
        return "ℹ️ Du bist bereits angemeldet."

    # Warteliste prüfen
    driver_count = get_registration_count(race_id)
    grid_count = calculate_grids(driver_count)
    max_drivers = grid_count * DRIVERS_PER_GRID
    on_waitlist = state.get("grid_locked") and driver_count >= max_drivers

    add_registration(race_id, driver_id, source="manual")
    add_log_entry(race_id, driver_id, "warteliste" if on_waitlist else "angemeldet")

    # Sheet-Sync
    if not TEST_MODE:
        sync_registrations_to_sheet(race_id)

    # Channel-Nachricht aktualisieren
    await update_checkin_message(session)
    await flush_orga_messages(session)

    # Grid-Full prüfen
    new_count = get_registration_count(race_id)
    new_grids = calculate_grids(new_count)
    if new_grids > state.get("last_grid_count", 0) and not state.get("grid_locked"):
        await send_grid_full_msg(session, new_grids)
        state["last_grid_count"] = new_grids

    if on_waitlist:
        await send_waitlist_msg(session, [driver.get("psn_name", nickname)])
        return "✅ Du hast dich angemeldet und stehst auf der Warteliste."

    return "✅ Du hast dich zum Rennen angemeldet."


async def handle_unregister(session, discord_id: str, nickname: str) -> str:
    """Verarbeitet den Abmelden-Button."""
    from db import (
        get_registration, remove_registration, add_log_entry,
        get_registration_count
    )
    from driver_resolver import resolve_driver
    from sheets import sync_registrations_to_sheet

    race_id = state.get("current_race_id")
    if not race_id:
        return "❌ Kein aktives Rennen gefunden."

    driver = resolve_driver(discord_id, nickname, queue_orga_message)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden."

    driver_id = driver["driver_id"]
    reg = get_registration(race_id, driver_id)

    if not reg:
        return "ℹ️ Du bist nicht angemeldet."

    # Wartelisten-Status bestimmen
    driver_count = get_registration_count(race_id)
    grid_count = calculate_grids(driver_count)
    max_drivers = grid_count * DRIVERS_PER_GRID
    was_on_waitlist = driver_count > max_drivers

    # Prüfen ob jemand nachrückt
    all_regs = []
    if was_on_waitlist:
        from db import get_all_registrations
        all_regs = get_all_registrations(race_id)

    remove_registration(race_id, driver_id)

    action = "warteliste_abgemeldet" if was_on_waitlist else "abgemeldet"
    add_log_entry(race_id, driver_id, action)

    # Nachrücker ermitteln
    if was_on_waitlist and all_regs:
        # Letzter auf Warteliste rückt nach
        waitlist_drivers = all_regs[max_drivers:]
        if waitlist_drivers:
            moved_up = waitlist_drivers[0]
            add_log_entry(race_id, moved_up["driver_id"], "nachgerueckt")
            await send_moved_up_msg(session, [moved_up.get("psn_name", "")])

    if not TEST_MODE:
        sync_registrations_to_sheet(race_id)

    await update_checkin_message(session)
    await flush_orga_messages(session)

    if was_on_waitlist:
        return "❌ Du hast dich von der Warteliste abgemeldet."
    return "❌ Du hast dich vom Rennen abgemeldet."


async def handle_abo_add(session, discord_id: str, nickname: str) -> str:
    """Verarbeitet den Daueranmeldung-Button."""
    from db import (
        has_abo, add_abo, get_registration, add_registration,
        add_log_entry, get_registration_count
    )
    from driver_resolver import resolve_driver
    from sheets import sync_registrations_to_sheet

    race_id = state.get("current_race_id")
    driver = resolve_driver(discord_id, nickname, queue_orga_message)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden."

    driver_id = driver["driver_id"]

    if has_abo(driver_id):
        return "ℹ️ Du hast bereits eine Daueranmeldung."

    add_abo(driver_id)
    add_log_entry(race_id, driver_id, "abo_angemeldet")

    # Falls noch nicht angemeldet → jetzt eintragen
    already_registered = race_id and get_registration(race_id, driver_id)
    if race_id and not already_registered:
        add_registration(race_id, driver_id, source="abo")
        if not TEST_MODE:
            sync_registrations_to_sheet(race_id)
        await update_checkin_message(session)
        await flush_orga_messages(session)
        return "✅ Du bist jetzt dauerhaft angemeldet und wurdest automatisch für dieses Rennen eingetragen."

    await update_checkin_message(session)
    return "✅ Du bist jetzt dauerhaft angemeldet und bleibst für dieses Rennen angemeldet."


async def handle_abo_remove(session, discord_id: str, nickname: str) -> str:
    """Verarbeitet den Dauerabmeldung-Button."""
    from db import has_abo, remove_abo, get_registration, add_log_entry
    from driver_resolver import resolve_driver

    race_id = state.get("current_race_id")
    driver = resolve_driver(discord_id, nickname, queue_orga_message)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden."

    driver_id = driver["driver_id"]

    if not has_abo(driver_id):
        return "ℹ️ Du hast keine aktive Daueranmeldung."

    remove_abo(driver_id)
    if race_id:
        add_log_entry(race_id, driver_id, "abo_abgemeldet")

    still_registered = race_id and get_registration(race_id, driver_id)
    if still_registered:
        return (
            "❌ Du hast dich von der Daueranmeldung abgemeldet. "
            "Du bist für das aktuelle Rennen noch angemeldet. "
            "Bitte melde dich separat ab, falls du nicht mitfahren möchtest."
        )
    return (
        "❌ Du hast dich von der Daueranmeldung abgemeldet. "
        "Du wirst ab nächster Woche nicht mehr automatisch eingetragen."
    )


async def handle_status(discord_id: str, nickname: str) -> str:
    """Verarbeitet den Status-Button."""
    from db import get_registration, has_abo
    from driver_resolver import resolve_driver
    from message_builder import build_status_message

    race_id = state.get("current_race_id")
    race = state.get("current_race")

    driver = resolve_driver(discord_id, nickname, queue_orga_message)
    if not driver:
        return "❌ Dein Profil wurde nicht gefunden."

    if not race_id or not race:
        reg = get_registration(race_id, driver["driver_id"]) if race_id else None
        abo = has_abo(driver["driver_id"])
        status = "✅ Angemeldet" if reg else "❌ Nicht angemeldet"
        abo_text = " · 📋 Dauerabo aktiv" if abo else ""
        return f"{status}{abo_text}\n\nKein aktives Rennen."

    return build_status_message(driver, race_id, race)


# ─────────────────────────────────────────────
# Orga-Commands
# ─────────────────────────────────────────────

async def handle_setgrids(session, channel_id: str, message_id: str,
                           discord_id: str, args: str):
    """Verarbeitet den !setgrids Befehl."""
    from db import set_grid_override
    race_id = state.get("current_race_id")
    if not race_id:
        await send_message(session, CHAN_ORGA, "❌ Kein aktives Rennen.")
        return

    try:
        count = int(args.strip())
    except ValueError:
        await send_message(session, CHAN_ORGA, "❌ Ungültige Zahl. Beispiel: `!setgrids 4`")
        return

    set_grid_override(race_id, count, discord_id)
    await send_message(session, CHAN_ORGA, f"✅ Grid-Anzahl manuell auf **{count}** gesetzt.")
    await update_checkin_message(session)
    await delete_message(session, channel_id, message_id)


async def handle_help(session):
    """Sendet den Hilfetext."""
    await send_message(session, CHAN_ORGA, MSG_HILFETEXT or "Kein Hilfetext konfiguriert.")


# ─────────────────────────────────────────────
# Dienstags-Reset
# ─────────────────────────────────────────────

async def tuesday_reset(session):
    """Führt den Dienstags-Reset um 10:00 Uhr durch."""
    from db import (
        get_next_monday_race, clear_registrations,
        get_all_abos, add_registration, add_log_entry,
        get_next_monday_is_pause
    )
    from sheets import sync_registrations_to_sheet, clear_lobby_codes_sheet

    log.info("Dienstags-Reset gestartet.")

    # ── 1. Lobby-Code-Channel leeren + neu posten ────────────────────────
    if not TEST_MODE:
        await bulk_delete_messages(session, CHAN_CODES, token=DISCORD_TOKEN_LOBBYCODEGRABBER)
        await send_message(session, CHAN_CODES, MSG_LOBBYCODES, token=DISCORD_TOKEN_LOBBYCODEGRABBER)
        clear_lobby_codes_sheet()
    else:
        log.info("TEST_MODE – Lobby-Code-Channel nicht geleert.")

    # ── 2. State zurücksetzen ────────────────────────────────────────────
    state["sunday_lock"] = False
    state["sunday_msg_sent"] = False
    state["grid_locked"] = False
    state["last_grid_count"] = 0
    from db import save_state
    save_state({
        "sunday_lock": False,
        "sunday_msg_sent": False,
        "grid_locked": False,
        "last_grid_count": 0,
    })

    # ── 3. Nächstes Rennen laden ─────────────────────────────────────────
    race = get_next_monday_race()

    if race:
        race_id = race["id"]
        state["current_race_id"] = race_id
        state["current_race"] = race

        # ── 4. Alte Anmeldungen löschen ──────────────────────────────────
        clear_registrations(race_id)

        # ── 5. Dauerabo-Fahrer eintragen ─────────────────────────────────
        abos = get_all_abos()
        for abo in abos:
            add_registration(race_id, abo["driver_id"], source="abo")
            add_log_entry(race_id, abo["driver_id"], "abo_angemeldet")
        log.info(f"Dauerabo: {len(abos)} Fahrer eingetragen.")

        # ── 6. Sheet-Sync ─────────────────────────────────────────────────
        if not TEST_MODE:
            sync_registrations_to_sheet(race_id)

        # ── 7. News-Channel Nachricht ─────────────────────────────────────
        raw = MSG_NEW_EVENT_EN if ENABLE_MULTILANGUAGE else MSG_NEW_EVENT
        text = _pick_msg(raw)
        if text:
            await send_message(session, _news_channel(), text)

    else:
        state["current_race_id"] = None
        state["current_race"] = None

    # ── 8. Channel-Nachricht aktualisieren ───────────────────────────────
    await update_checkin_message(session)
    await flush_orga_messages(session)
    log.info("Dienstags-Reset abgeschlossen.")


# ─────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────

async def scheduler(session):
    """Prüft minütlich ob geplante Aktionen ausgeführt werden müssen."""
    last_tuesday_reset = None
    last_sunday_lock = None
    last_deadline_check = None

    while True:
        await asyncio.sleep(60)
        now = datetime.now(BERLIN)

        # ── Dienstag 10:00 – Reset ───────────────────────────────────────
        if now.weekday() == 1 and now.hour == 10 and now.minute == 0:
            key = now.strftime("%Y-%m-%d")
            if last_tuesday_reset != key:
                last_tuesday_reset = key
                await tuesday_reset(session)

        # ── Sonntag 18:00 – Grid-Lock ────────────────────────────────────
        if now.weekday() == 6 and now.hour == 18 and now.minute == 0:
            key = now.strftime("%Y-%m-%d")
            if last_sunday_lock != key:
                last_sunday_lock = key
                await send_sunday_msg(session)
                await update_checkin_message(session)

        # ── Montag REGISTRATION_DEADLINE – Rote Ampel ────────────────────
        if now.weekday() == 0:
            h, m = map(int, REGISTRATION_DEADLINE.split(":"))
            if now.hour == h and now.minute == m:
                key = now.strftime("%Y-%m-%d")
                if last_deadline_check != key:
                    last_deadline_check = key
                    log.info("Anmeldeschluss erreicht – schalte auf 🔴.")
                    await update_checkin_message(session)

        # ── Channel-Nachricht stündlich aktualisieren ────────────────────
        if now.minute == 0:
            await update_checkin_message(session)


# ─────────────────────────────────────────────
# Flask – Discord Interactions Endpoint
# ─────────────────────────────────────────────

flask_app = Flask(__name__)
interaction_queue: asyncio.Queue = None


@flask_app.route("/interactions", methods=["POST"])
def interactions():
    """Discord Interactions Webhook Endpoint."""
    # Signaturverifikation
    signature = request.headers.get("X-Signature-Ed25519", "")
    timestamp = request.headers.get("X-Signature-Timestamp", "")
    body = request.data

    try:
        verify_key = VerifyKey(bytes.fromhex(DISCORD_PUBLIC_KEY))
        verify_key.verify(timestamp.encode() + body, bytes.fromhex(signature))
    except BadSignatureError:
        return jsonify({"error": "invalid signature"}), 401

    data = request.json

    # PING
    if data.get("type") == 1:
        return jsonify({"type": 1})

    # Button-Interaction
    if data.get("type") == 3:
        interaction_queue.put_nowait(data)
        return jsonify({"type": 5})  # Deferred response

    return jsonify({"type": 1})


def launch_flask_thread():
    """Startet Flask in einem Daemon-Thread."""
    def run():
        flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
    t = threading.Thread(target=run, daemon=True)
    t.start()
    log.info(f"Flask läuft auf Port {PORT}.")


# ─────────────────────────────────────────────
# Interaction-Handler
# ─────────────────────────────────────────────

async def process_interactions(session):
    """Verarbeitet eingehende Discord Interactions aus der Queue."""
    while True:
        try:
            data = await asyncio.wait_for(interaction_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        custom_id = data.get("data", {}).get("custom_id", "")
        user = data.get("member", {}).get("user", {})
        discord_id = user.get("id", "")
        nickname = data.get("member", {}).get("nick") or user.get("username", "")
        interaction_id = data.get("id", "")
        interaction_token = data.get("token", "")

        log.info(f"Interaction: {custom_id} von {nickname} ({discord_id})")

        if custom_id == "checkin_register":
            response = await handle_register(session, discord_id, nickname)
        elif custom_id == "checkin_unregister":
            response = await handle_unregister(session, discord_id, nickname)
        elif custom_id == "checkin_abo_add":
            response = await handle_abo_add(session, discord_id, nickname)
        elif custom_id == "checkin_abo_remove":
            response = await handle_abo_remove(session, discord_id, nickname)
        elif custom_id == "checkin_status":
            response = await handle_status(discord_id, nickname)
        else:
            response = "❌ Unbekannte Aktion."

        # Ephemeral followup senden
        await send_ephemeral_followup(interaction_token, response)


async def send_ephemeral_followup(token: str, content: str):
    """Sendet eine ephemeral Followup-Nachricht."""
    url = f"https://discord.com/api/v10/webhooks/{_env('DISCORD_APPLICATION_ID', '')}/{token}"
    payload = {"content": content, "flags": 64}  # 64 = ephemeral
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload) as resp:
            if resp.status not in (200, 204):
                text = await resp.text()
                log.error(f"Ephemeral followup fehlgeschlagen: {resp.status} {text}")


# ─────────────────────────────────────────────
# Orga-Command Scanner
# ─────────────────────────────────────────────

async def scan_orga_commands(session):
    """Scannt den Orga-Channel auf Bot-Commands."""
    last_message_id = None

    while True:
        await asyncio.sleep(10)
        try:
            endpoint = f"/channels/{CHAN_ORGA}/messages?limit=10"
            if last_message_id:
                endpoint += f"&after={last_message_id}"

            msgs = await discord_request(session, "GET", endpoint)
            if not msgs:
                continue

            for msg in reversed(msgs):
                last_message_id = msg["id"]
                content = msg.get("content", "").strip()
                author = msg.get("author", {})
                author_id = author.get("id", "")

                # Nur Orga-Mitglieder
                if author_id not in USER_ID_ORGA:
                    continue

                if content.startswith("!setgrids "):
                    args = content[len("!setgrids "):]
                    await handle_setgrids(session, CHAN_ORGA, msg["id"], author_id, args)
                elif content.strip() == "!help":
                    await handle_help(session)
                    await delete_message(session, CHAN_ORGA, msg["id"])

        except Exception as e:
            log.error(f"Command-Scan Fehler: {e}")


# ─────────────────────────────────────────────
# Bootstrap (erster Start)
# ─────────────────────────────────────────────

async def bootstrap(session):
    """Initialisierung beim ersten Start – lädt State aus DB."""
    from db import get_next_monday_race, get_race_by_id, load_state, save_state

    log.info("Bootstrap gestartet.")

    # State aus DB laden
    db_state = load_state()

    # sunday_lock / grid_locked wiederherstellen
    state["sunday_lock"]     = db_state.get("sunday_lock", "False") == "True"
    state["sunday_msg_sent"] = db_state.get("sunday_msg_sent", "False") == "True"
    state["grid_locked"]     = db_state.get("grid_locked", "False") == "True"
    state["last_grid_count"] = int(db_state.get("last_grid_count", 0))

    # Rennen wiederherstellen
    saved_race_id = db_state.get("current_race_id")
    if saved_race_id:
        race = get_race_by_id(int(saved_race_id))
        if race:
            state["current_race_id"] = race["id"]
            state["current_race"]    = race
            log.info(f"State wiederhergestellt: {race['track_name']} am {race['race_date']}")

    # Falls kein State → nächstes Rennen laden
    if not state.get("current_race_id"):
        race = get_next_monday_race()
        if race:
            state["current_race_id"] = race["id"]
            state["current_race"]    = race
            log.info(f"Aktives Rennen: {race['track_name']} am {race['race_date']}")
        else:
            log.info("Kein Rennen nächsten Montag.")

    await update_checkin_message(session)
    log.info("Bootstrap abgeschlossen.")


# ─────────────────────────────────────────────
# Worker
# ─────────────────────────────────────────────

async def worker():
    """Haupt-Async-Worker."""
    global interaction_queue
    interaction_queue = asyncio.Queue()

    async with aiohttp.ClientSession() as session:
        await bootstrap(session)

        await asyncio.gather(
            scheduler(session),
            process_interactions(session),
            scan_orga_commands(session),
        )


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # Application ID für ephemeral followups
    # Wird aus dem Token extrahiert (erste Teil des Tokens = Base64 der App-ID)
    import base64
    try:
        token_parts = DISCORD_TOKEN_CHECKINBOT.split(".")
        app_id = base64.b64decode(token_parts[0] + "==").decode("utf-8")
        os.environ["DISCORD_APPLICATION_ID"] = app_id
    except Exception:
        log.warning("Application-ID konnte nicht aus Token extrahiert werden.")

    launch_flask_thread()

    try:
        asyncio.run(worker())
    except KeyboardInterrupt:
        log.info("Shutdown durch KeyboardInterrupt.")
