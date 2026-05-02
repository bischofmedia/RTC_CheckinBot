"""
RTC CheckinBot – checkin_bot.py
Hauptbot: Discord.py Gateway, Buttons, Wochenlogik, Scheduler
"""

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

import discord
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/ubuntu/RTC_CheckinBot/.env")

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
DISCORD_TOKEN                  = _env("DISCORD_TOKEN_CHECKINBOT", "")
DISCORD_TOKEN_LOBBYCODEGRABBER = _env("DISCORD_TOKEN_LOBBYCODEGRABBER", "")
DISCORD_GUILD_ID               = int(_env("DISCORD_GUILD_ID", "0"))
CHAN_CHECKIN                   = int(_env("CHAN_CHECKIN", "0"))
CHAN_CHECKIN_MSG_ID             = _env("CHAN_CHECKIN_MSG_ID", "") or None
CHAN_NEWS                      = int(_env("CHAN_NEWS", "0"))
CHAN_CODES                     = int(_env("CHAN_CODES", "0"))
CHAN_ORGA                      = int(_env("CHAN_ORGA", "0"))
USER_ID_ORGA                   = [int(u.strip()) for u in _env("USER_ID_ORGA", "").split(";") if u.strip()]
DRIVERS_PER_GRID               = _env_int("DRIVERS_PER_GRID", 15)
MAX_GRIDS                      = _env_int("MAX_GRIDS", 4)
REGISTRATION_DEADLINE          = _env("REGISTRATION_DEADLINE", "20:45")
LOBBY_OPEN                     = _env("LOBBY_OPEN", "20:30")
TEST_MODE                      = _env("TEST_MODE", "false").lower() == "true"

ENABLE_EXTRA_GRID     = _env_int("ENABLE_EXTRA_GRID", 0)
EXTRA_GRID_THRESHOLD  = _env_int("EXTRA_GRID_THRESHOLD", 10)
ENABLE_EXTRA_GRID_MSG = _env_int("ENABLE_EXTRA_GRID_MSG", 1)
ENABLE_MOVED_UP_MSG   = _env_int("ENABLE_MOVED_UP_MSG", 1)
ENABLE_WAITLIST_MSG   = _env_int("ENABLE_WAITLIST_MSG", 1)
ENABLE_SUNDAY_MSG     = _env_int("ENABLE_SUNDAY_MSG", 1)
ENABLE_GRID_FULL_MSG  = _env_int("ENABLE_GRID_FULL_MSG", 1)
ENABLE_MULTILANGUAGE  = _env_int("ENABLE_MULTILANGUAGE", 0)
SET_MIN_GRIDS_MSG     = _env_int("SET_MIN_GRIDS_MSG", 3)

MSG_EXTRA_GRID_TEXT     = _env_msg("MSG_EXTRA_GRID_TEXT")
MSG_EXTRA_GRID_TEXT_EN  = _env_msg("MSG_EXTRA_GRID_TEXT_EN")
MSG_GRID_FULL_TEXT      = _env_msg("MSG_GRID_FULL_TEXT")
MSG_GRID_FULL_TEXT_EN   = _env_msg("MSG_GRID_FULL_TEXT_EN")
MSG_MOVED_UP_SINGLE     = _env_msg("MSG_MOVED_UP_SINGLE")
MSG_MOVED_UP_SINGLE_EN  = _env_msg("MSG_MOVED_UP_SINGLE_EN")
MSG_MOVED_UP_MULTI      = _env_msg("MSG_MOVED_UP_MULTI")
MSG_MOVED_UP_MULTI_EN   = _env_msg("MSG_MOVED_UP_MULTI_EN")
MSG_SUNDAY_TEXT         = _env_msg("MSG_SUNDAY_TEXT")
MSG_SUNDAY_TEXT_EN      = _env_msg("MSG_SUNDAY_TEXT_EN")
MSG_WAITLIST_SINGLE     = _env_msg("MSG_WAITLIST_SINGLE")
MSG_WAITLIST_SINGLE_EN  = _env_msg("MSG_WAITLIST_SINGLE_EN")
MSG_WAITLIST_MULTI      = _env_msg("MSG_WAITLIST_MULTI")
MSG_WAITLIST_MULTI_EN   = _env_msg("MSG_WAITLIST_MULTI_EN")
MSG_NEW_EVENT           = _env_msg("MSG_NEW_EVENT")
MSG_NEW_EVENT_EN        = _env_msg("MSG_NEW_EVENT_EN")
MSG_NEW_EVENT_TEXT      = _env_msg("MSG_NEW_EVENT_TEXT")
MSG_NEW_EVENT_TEXT_EN   = _env_msg("MSG_NEW_EVENT_TEXT_EN")
MSG_LOBBYCODES          = _env_msg("MSG_LOBBYCODES")
MSG_HILFETEXT           = _env_msg("MSG_HILFETEXT")
MSG_GRID_CHANGE_TEXT    = _env_msg("MSG_GRID_CHANGE_TEXT")
MSG_GRID_CHANGE_TEXT_EN = _env_msg("MSG_GRID_CHANGE_TEXT_EN")

# ─────────────────────────────────────────────
# State
# ─────────────────────────────────────────────
state = {
    "current_race_id": None,
    "current_race": None,
    "checkin_msg_id": int(CHAN_CHECKIN_MSG_ID) if CHAN_CHECKIN_MSG_ID else None,
    "sunday_lock": False,
    "sunday_msg_sent": False,
    "last_grid_count": 0,
    "grid_locked": False,
}

# ─────────────────────────────────────────────
# Discord Client
# ─────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = discord.Client(intents=intents)

# ─────────────────────────────────────────────
# Hilfsfunktionen
# ─────────────────────────────────────────────

def _news_channel():
    return CHAN_CHECKIN if TEST_MODE else CHAN_NEWS

def _codes_channel():
    return CHAN_CHECKIN if TEST_MODE else CHAN_CODES

def calculate_grids(driver_count: int) -> int:
    from db import get_grid_override
    race_id = state.get("current_race_id")
    if race_id:
        override = get_grid_override(race_id)
        if override:
            return override["grid_count"]
    if driver_count == 0:
        return 0
    grids = max(1, driver_count // DRIVERS_PER_GRID)
    if ENABLE_EXTRA_GRID:
        remainder = driver_count % DRIVERS_PER_GRID
        if remainder >= EXTRA_GRID_THRESHOLD:
            grids += 1
    return min(grids, MAX_GRIDS)

def is_registration_closed() -> bool:
    now = datetime.now(BERLIN)
    if now.weekday() != 0:
        return False
    h, m = map(int, REGISTRATION_DEADLINE.split(":"))
    deadline = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return now >= deadline

def _save_msg_id(msg_id: int):
    try:
        env_path = "/home/ubuntu/RTC_CheckinBot/.env"
        with open(env_path, "r") as f:
            lines = f.readlines()
        with open(env_path, "w") as f:
            found = False
            for line in lines:
                if line.startswith("CHAN_CHECKIN_MSG_ID="):
                    f.write(f"CHAN_CHECKIN_MSG_ID={msg_id}\n")
                    found = True
                else:
                    f.write(line)
            if not found:
                f.write(f"CHAN_CHECKIN_MSG_ID={msg_id}\n")
        state["checkin_msg_id"] = msg_id
        log.info(f"Message-ID {msg_id} in .env gespeichert.")
    except Exception as e:
        log.error(f"Fehler beim Speichern der Message-ID: {e}")

# ─────────────────────────────────────────────
# Channel-Nachricht aktualisieren
# ─────────────────────────────────────────────

async def update_checkin_message():
    from message_builder import build_channel_message
    race = state.get("current_race")
    race_id = state.get("current_race_id")
    message, show_buttons = build_channel_message(race_id=race_id, race=race)

    channel = bot.get_channel(CHAN_CHECKIN)
    if not channel:
        log.error("CHAN_CHECKIN nicht gefunden!")
        return

    view = CheckinView() if show_buttons else None
    msg_id = state.get("checkin_msg_id")

    if msg_id:
        try:
            msg = await channel.fetch_message(msg_id)
            await msg.edit(content=message, view=view)
            return
        except discord.NotFound:
            state["checkin_msg_id"] = None

    # Channel leeren vor neuem Post
    try:
        async for old_msg in channel.history(limit=50):
            await old_msg.delete()
    except Exception as e:
        log.warning(f"Channel-Bereinigung fehlgeschlagen: {e}")

    msg = await channel.send(content=message, view=view)
    _save_msg_id(msg.id)
    log.info(f"Neue Checkin-Nachricht gepostet: {msg.id}")

# ─────────────────────────────────────────────
# Views & Buttons
# ─────────────────────────────────────────────

class CheckinView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Anmelden", style=discord.ButtonStyle.success, custom_id="checkin_register")
    async def register(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        response, view = await handle_register(interaction)
        if view:
            await interaction.followup.send(response, view=view, ephemeral=True)
        else:
            await interaction.followup.send(response, ephemeral=True)

    @discord.ui.button(label="Abmelden", style=discord.ButtonStyle.danger, custom_id="checkin_unregister")
    async def unregister(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        response, view = await handle_unregister(interaction)
        if view:
            await interaction.followup.send(response, view=view, ephemeral=True)
        else:
            await interaction.followup.send(response, ephemeral=True)

    @discord.ui.button(label="Status", style=discord.ButtonStyle.secondary, custom_id="checkin_status")
    async def status(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        response, _ = await handle_status(interaction)
        await interaction.followup.send(response, ephemeral=True)


class AboAddView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Daueranmeldung", style=discord.ButtonStyle.primary, custom_id="checkin_abo_add")
    async def abo_add(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        response, _ = await handle_abo_add(interaction)
        await interaction.followup.send(response, ephemeral=True)


class AboRemoveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Dauerabmeldung", style=discord.ButtonStyle.danger, custom_id="checkin_abo_remove")
    async def abo_remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        response, _ = await handle_abo_remove(interaction)
        await interaction.followup.send(response, ephemeral=True)


# ─────────────────────────────────────────────
# Fahrer-Auflösung
# ─────────────────────────────────────────────

async def resolve_driver_from_interaction(interaction: discord.Interaction):
    from driver_resolver import resolve_driver
    discord_id = str(interaction.user.id)
    member = interaction.user
    nickname = member.nick if hasattr(member, 'nick') and member.nick else member.name

    orga_messages = []
    def queue_msg(text):
        orga_messages.append(text)

    driver = resolve_driver(discord_id, nickname, queue_msg)

    if orga_messages:
        channel = bot.get_channel(CHAN_ORGA)
        if channel:
            for msg in orga_messages:
                await channel.send(msg)

    return driver

# ─────────────────────────────────────────────
# Button-Handler
# ─────────────────────────────────────────────

async def handle_register(interaction: discord.Interaction):
    from db import (
        get_registration, add_registration, add_log_entry,
        get_registration_count, has_abo
    )
    from sheets import sync_registrations_to_sheet

    race_id = state.get("current_race_id")
    if not race_id:
        return "❌ Kein aktives Rennen gefunden.", None

    driver = await resolve_driver_from_interaction(interaction)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden. Die Orga wurde informiert.", None

    driver_id = driver["driver_id"]

    if get_registration(race_id, driver_id):
        return "ℹ️ Du bist bereits angemeldet.", None

    driver_count = get_registration_count(race_id)
    grid_count = calculate_grids(driver_count)
    max_drivers = grid_count * DRIVERS_PER_GRID
    on_waitlist = state.get("grid_locked") and driver_count >= max_drivers

    add_registration(race_id, driver_id, source="manual")
    add_log_entry(race_id, driver_id, "warteliste" if on_waitlist else "angemeldet")

    if not TEST_MODE:
        sync_registrations_to_sheet(race_id)

    await update_checkin_message()

    new_count = get_registration_count(race_id)
    new_grids = calculate_grids(new_count)
    if new_grids > state.get("last_grid_count", 0) and not state.get("grid_locked"):
        await send_grid_full_msg(new_grids)
        state["last_grid_count"] = new_grids

    if on_waitlist:
        await send_waitlist_msg([driver.get("psn_name", "")])
        msg = "✅ Du hast dich angemeldet und stehst auf der Warteliste."
    else:
        msg = "✅ Du hast dich zum Rennen angemeldet."

    view = None
    if not has_abo(driver_id):
        msg += "\n\n📋 Möchtest du dich dauerhaft anmelden?"
        view = AboAddView()

    return msg, view


async def handle_unregister(interaction: discord.Interaction):
    from db import (
        get_registration, remove_registration, add_log_entry,
        get_registration_count, get_all_registrations, has_abo
    )
    from sheets import sync_registrations_to_sheet

    race_id = state.get("current_race_id")
    if not race_id:
        return "❌ Kein aktives Rennen gefunden.", None

    driver = await resolve_driver_from_interaction(interaction)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden.", None

    driver_id = driver["driver_id"]
    reg = get_registration(race_id, driver_id)

    if not reg:
        return "ℹ️ Du bist nicht angemeldet.", None

    driver_count = get_registration_count(race_id)
    grid_count = calculate_grids(driver_count)
    max_drivers = grid_count * DRIVERS_PER_GRID
    was_on_waitlist = driver_count > max_drivers

    all_regs = get_all_registrations(race_id) if was_on_waitlist else []
    remove_registration(race_id, driver_id)

    action = "warteliste_abgemeldet" if was_on_waitlist else "abgemeldet"
    add_log_entry(race_id, driver_id, action)

    if was_on_waitlist and all_regs:
        waitlist_drivers = all_regs[max_drivers:]
        if waitlist_drivers:
            moved_up = waitlist_drivers[0]
            add_log_entry(race_id, moved_up["driver_id"], "nachgerueckt")
            await send_moved_up_msg([moved_up.get("psn_name", "")])

    if not TEST_MODE:
        sync_registrations_to_sheet(race_id)

    await update_checkin_message()

    if was_on_waitlist:
        msg = "❌ Du hast dich von der Warteliste abgemeldet."
    else:
        msg = "❌ Du hast dich vom Rennen abgemeldet."

    view = None
    if has_abo(driver_id):
        msg += "\n\n📋 Möchtest du auch deine Daueranmeldung beenden?"
        view = AboRemoveView()

    return msg, view


async def handle_abo_add(interaction: discord.Interaction):
    from db import has_abo, add_abo, get_registration, add_registration, add_log_entry
    from sheets import sync_registrations_to_sheet

    race_id = state.get("current_race_id")
    driver = await resolve_driver_from_interaction(interaction)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden.", None

    driver_id = driver["driver_id"]

    if has_abo(driver_id):
        return "ℹ️ Du hast bereits eine Daueranmeldung.", None

    add_abo(driver_id)
    if race_id:
        add_log_entry(race_id, driver_id, "abo_angemeldet")

    already_registered = race_id and get_registration(race_id, driver_id)
    if race_id and not already_registered:
        add_registration(race_id, driver_id, source="abo")
        if not TEST_MODE:
            sync_registrations_to_sheet(race_id)
        await update_checkin_message()
        return "✅ Du bist jetzt dauerhaft angemeldet und wurdest automatisch für dieses Rennen eingetragen.", None

    await update_checkin_message()
    return "✅ Du bist jetzt dauerhaft angemeldet und bleibst für dieses Rennen angemeldet.", None


async def handle_abo_remove(interaction: discord.Interaction):
    from db import has_abo, remove_abo, get_registration, add_log_entry

    race_id = state.get("current_race_id")
    driver = await resolve_driver_from_interaction(interaction)
    if not driver:
        return "❌ Dein Profil konnte nicht aufgelöst werden.", None

    driver_id = driver["driver_id"]

    if not has_abo(driver_id):
        return "ℹ️ Du hast keine aktive Daueranmeldung.", None

    remove_abo(driver_id)
    if race_id:
        add_log_entry(race_id, driver_id, "abo_abgemeldet")

    still_registered = race_id and get_registration(race_id, driver_id)
    if still_registered:
        return (
            "❌ Du hast dich von der Daueranmeldung abgemeldet. "
            "Du bist für das aktuelle Rennen noch angemeldet. "
            "Bitte melde dich separat ab, falls du nicht mitfahren möchtest."
        ), None
    return (
        "❌ Du hast dich von der Daueranmeldung abgemeldet. "
        "Du wirst ab nächster Woche nicht mehr automatisch eingetragen."
    ), None


async def handle_status(interaction: discord.Interaction):
    from db import get_registration, has_abo
    from driver_resolver import resolve_driver
    from message_builder import build_status_message

    race_id = state.get("current_race_id")
    race = state.get("current_race")

    discord_id = str(interaction.user.id)
    member = interaction.user
    nickname = member.nick if hasattr(member, 'nick') and member.nick else member.name

    driver = resolve_driver(discord_id, nickname)
    if not driver:
        return "❌ Dein Profil wurde nicht gefunden.", None

    if not race_id or not race:
        reg = get_registration(race_id, driver["driver_id"]) if race_id else None
        abo = has_abo(driver["driver_id"])
        status = "✅ Angemeldet" if reg else "❌ Nicht angemeldet"
        abo_text = " · 📋 Dauerabo aktiv" if abo else ""
        return f"{status}{abo_text}\n\nKein aktives Rennen.", None

    return build_status_message(driver, race_id, race), None

# ─────────────────────────────────────────────
# Nachrichten-Helfer
# ─────────────────────────────────────────────

async def send_grid_full_msg(new_grids: int):
    if not ENABLE_GRID_FULL_MSG or new_grids < SET_MIN_GRIDS_MSG:
        return
    raw = MSG_GRID_FULL_TEXT_EN if ENABLE_MULTILANGUAGE else MSG_GRID_FULL_TEXT
    text = _pick_msg(raw, full_grids=new_grids)
    if text:
        channel = bot.get_channel(_news_channel())
        if channel:
            await channel.send(text)


async def send_waitlist_msg(driver_names: list):
    if not ENABLE_WAITLIST_MSG:
        return
    names_str = ", ".join(driver_names)
    if len(driver_names) == 1:
        raw = MSG_WAITLIST_SINGLE_EN if ENABLE_MULTILANGUAGE else MSG_WAITLIST_SINGLE
    else:
        raw = MSG_WAITLIST_MULTI_EN if ENABLE_MULTILANGUAGE else MSG_WAITLIST_MULTI
    text = _pick_msg(raw, driver_names=names_str)
    if text:
        channel = bot.get_channel(_news_channel())
        if channel:
            await channel.send(text)


async def send_moved_up_msg(driver_names: list):
    if not ENABLE_MOVED_UP_MSG:
        return
    names_str = ", ".join(driver_names)
    if len(driver_names) == 1:
        raw = MSG_MOVED_UP_SINGLE_EN if ENABLE_MULTILANGUAGE else MSG_MOVED_UP_SINGLE
    else:
        raw = MSG_MOVED_UP_MULTI_EN if ENABLE_MULTILANGUAGE else MSG_MOVED_UP_MULTI
    text = _pick_msg(raw, driver_names=names_str)
    if text:
        channel = bot.get_channel(_news_channel())
        if channel:
            await channel.send(text)


async def send_sunday_msg():
    if not ENABLE_SUNDAY_MSG or state.get("sunday_msg_sent"):
        return
    race_id = state.get("current_race_id")
    if not race_id:
        return
    from db import get_registration_count, save_state
    driver_count = get_registration_count(race_id)
    grid_count = calculate_grids(driver_count)
    free_slots = max(0, grid_count * DRIVERS_PER_GRID - driver_count)
    raw = MSG_SUNDAY_TEXT_EN if ENABLE_MULTILANGUAGE else MSG_SUNDAY_TEXT
    text = _pick_msg(raw, grids=grid_count, driver_count=driver_count, free_slots=free_slots)
    if text:
        channel = bot.get_channel(_news_channel())
        if channel:
            await channel.send(text)
    state["sunday_msg_sent"] = True
    state["grid_locked"] = True
    save_state({"sunday_msg_sent": True, "grid_locked": True, "last_grid_count": grid_count})
    log.info(f"Sunday-Lock: {grid_count} Grids, {driver_count} Fahrer.")

# ─────────────────────────────────────────────
# Dienstags-Reset
# ─────────────────────────────────────────────

async def tuesday_reset():
    from db import (
        get_next_monday_race, clear_registrations,
        get_all_abos, add_registration, add_log_entry, save_state
    )
    from sheets import sync_registrations_to_sheet, clear_lobby_codes_sheet

    log.info("Dienstags-Reset gestartet.")

    # Lobby-Code-Channel leeren (mit eigenem Token)
    if not TEST_MODE:
        try:
            codes_channel = bot.get_channel(CHAN_CODES)
            if codes_channel:
                async for message in codes_channel.history(limit=100):
                    await message.delete()
                await codes_channel.send(MSG_LOBBYCODES)
            clear_lobby_codes_sheet()
        except Exception as e:
            log.error(f"Lobby-Reset Fehler: {e}")

    # State zurücksetzen
    state["sunday_lock"] = False
    state["sunday_msg_sent"] = False
    state["grid_locked"] = False
    state["last_grid_count"] = 0
    save_state({"sunday_lock": False, "sunday_msg_sent": False,
                "grid_locked": False, "last_grid_count": 0})

    # Nächstes Rennen laden
    race = get_next_monday_race()
    if race:
        race_id = race["id"]
        state["current_race_id"] = race_id
        state["current_race"] = race
        save_state({"current_race_id": race_id})

        clear_registrations(race_id)

        abos = get_all_abos()
        for abo in abos:
            add_registration(race_id, abo["driver_id"], source="abo")
            add_log_entry(race_id, abo["driver_id"], "abo_angemeldet")
        log.info(f"Dauerabo: {len(abos)} Fahrer eingetragen.")

        if not TEST_MODE:
            sync_registrations_to_sheet(race_id)

        raw = MSG_NEW_EVENT_EN if ENABLE_MULTILANGUAGE else MSG_NEW_EVENT
        text = _pick_msg(raw)
        if text:
            channel = bot.get_channel(_news_channel())
            if channel:
                await channel.send(text)
    else:
        state["current_race_id"] = None
        state["current_race"] = None

    await update_checkin_message()
    log.info("Dienstags-Reset abgeschlossen.")

# ─────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────

_last_tuesday_reset = None
_last_sunday_lock = None
_last_deadline_check = None

@tasks.loop(minutes=1)
async def scheduler():
    global _last_tuesday_reset, _last_sunday_lock, _last_deadline_check
    now = datetime.now(BERLIN)

    if now.weekday() == 1 and now.hour == 10 and now.minute == 0:
        key = now.strftime("%Y-%m-%d")
        if _last_tuesday_reset != key:
            _last_tuesday_reset = key
            await tuesday_reset()

    if now.weekday() == 6 and now.hour == 18 and now.minute == 0:
        key = now.strftime("%Y-%m-%d")
        if _last_sunday_lock != key:
            _last_sunday_lock = key
            await send_sunday_msg()
            await update_checkin_message()

    if now.weekday() == 0:
        h, m = map(int, REGISTRATION_DEADLINE.split(":"))
        if now.hour == h and now.minute == m:
            key = now.strftime("%Y-%m-%d")
            if _last_deadline_check != key:
                _last_deadline_check = key
                log.info("Anmeldeschluss erreicht – schalte auf 🔴.")
                await update_checkin_message()

    if now.minute == 0:
        await update_checkin_message()

# ─────────────────────────────────────────────
# Orga-Commands
# ─────────────────────────────────────────────

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.id != CHAN_ORGA:
        return
    if message.author.id not in USER_ID_ORGA:
        return

    content = message.content.strip()

    if content.startswith("!setgrids "):
        args = content[len("!setgrids "):]
        from db import set_grid_override
        race_id = state.get("current_race_id")
        if not race_id:
            await message.channel.send("❌ Kein aktives Rennen.")
            return
        try:
            count = int(args.strip())
        except ValueError:
            await message.channel.send("❌ Ungültige Zahl. Beispiel: `!setgrids 4`")
            return
        set_grid_override(race_id, count, str(message.author.id))
        await message.channel.send(f"✅ Grid-Anzahl manuell auf **{count}** gesetzt.")
        await message.delete()
        await update_checkin_message()

    elif content.strip() == "!help":
        await message.channel.send(MSG_HILFETEXT or "Kein Hilfetext konfiguriert.")
        await message.delete()

# ─────────────────────────────────────────────
# Bot Events
# ─────────────────────────────────────────────

@bot.event
async def on_ready():
    log.info(f"Bot eingeloggt als {bot.user}")

    bot.add_view(CheckinView())
    bot.add_view(AboAddView())
    bot.add_view(AboRemoveView())

    from db import load_state, get_race_by_id, get_next_monday_race, save_state

    db_state = load_state()
    state["sunday_lock"]     = db_state.get("sunday_lock", "False") == "True"
    state["sunday_msg_sent"] = db_state.get("sunday_msg_sent", "False") == "True"
    state["grid_locked"]     = db_state.get("grid_locked", "False") == "True"
    state["last_grid_count"] = int(db_state.get("last_grid_count", 0))

    saved_race_id = db_state.get("current_race_id")
    if saved_race_id:
        race = get_race_by_id(int(saved_race_id))
        if race:
            state["current_race_id"] = race["id"]
            state["current_race"] = race
            log.info(f"State wiederhergestellt: {race['track_name']} am {race['race_date']}")

    if not state.get("current_race_id"):
        race = get_next_monday_race()
        if race:
            state["current_race_id"] = race["id"]
            state["current_race"] = race
            save_state({"current_race_id": race["id"]})
            log.info(f"Aktives Rennen: {race['track_name']} am {race['race_date']}")
        else:
            log.info("Kein Rennen nächsten Montag.")

    await update_checkin_message()
    scheduler.start()
    log.info("Bootstrap abgeschlossen.")

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
