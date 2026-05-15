"""
RTC CheckinBot – driver_resolver.py
Fahrer-Aufloesung + automatisches Onboarding fuer neue Fahrer
"""

import logging
import os
import asyncio
import discord
import gspread
from google.oauth2.service_account import Credentials
from db import (
    get_driver_by_discord_id,
    get_driver_by_nickname,
    update_driver_discord_id,
    create_driver,
)

log = logging.getLogger("CheckinBot")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COL_PSN        = 2
COL_NICK       = 10
HEADER_ROW     = 6


# Google Sheets

def _get_sheet_client():
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
    )
    return gspread.authorize(creds)


def _get_drvr_worksheet():
    client = _get_sheet_client()
    sheet = client.open_by_key(os.environ["GOOGLE_SHEETS_ID"])
    return sheet.worksheet("DB_drvr")


def _find_in_sheet_by_nick(nickname: str) -> dict | None:
    try:
        ws = _get_drvr_worksheet()
        records = ws.get_all_values()
        for i, row in enumerate(records):
            if i <= HEADER_ROW:
                continue
            if len(row) > COL_NICK and row[COL_NICK].strip() == nickname.strip():
                psn        = row[COL_PSN].strip() if len(row) > COL_PSN else ""
                discord_id = row[-1].strip() if row else ""
                gt7_name   = row[-2].strip() if len(row) >= 2 else ""
                return {
                    "psn_name":   psn or nickname,
                    "discord_id": discord_id,
                    "gt7_name":   gt7_name,
                    "row_index":  i + 1,
                }
        return None
    except Exception as e:
        log.error(f"Sheet-Suche nach Nick '{nickname}' fehlgeschlagen: {e}")
        return None


def _update_sheet_discord_id(row_index: int, discord_id: str):
    try:
        ws = _get_drvr_worksheet()
        total_cols = len(ws.row_values(row_index))
        ws.update_cell(row_index, total_cols, discord_id)
        log.info(f"Discord-ID {discord_id} in Sheet Zeile {row_index} eingetragen.")
    except Exception as e:
        log.error(f"Sheet-Update Discord-ID fehlgeschlagen: {e}")


def _add_driver_to_sheet(discord_id: str, discord_name: str):
    try:
        ws = _get_drvr_worksheet()
        records = ws.get_all_values()
        next_row = len(records) + 1
        total_cols = len(records[HEADER_ROW]) if records else 110
        ws.update_cell(next_row, COL_NICK + 1, discord_name)
        ws.update_cell(next_row, total_cols, discord_id)
        log.info(f"Neuer Fahrer '{discord_name}' in Sheet Zeile {next_row} eingetragen.")
    except Exception as e:
        log.error(f"Sheet-Eintrag fuer neuen Fahrer fehlgeschlagen: {e}")


# DB-Hilfsfunktionen

def _get_taken_start_numbers() -> set:
    from db import get_connection
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT start_number FROM drivers "
                    "WHERE is_active = 1 AND start_number IS NOT NULL"
                )
                return {row["start_number"] for row in cur.fetchall()}
    except Exception as e:
        log.error(f"Startnummern-Abfrage fehlgeschlagen: {e}")
        return set()


def _get_hardware_list(hw_type: str) -> list:
    from db import get_connection
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT hardware_id, brand, model, ps5_native "
                    "FROM hardware WHERE type = %s ORDER BY brand, model",
                    (hw_type,)
                )
                return cur.fetchall()
    except Exception as e:
        log.error(f"Hardware-Abfrage ({hw_type}) fehlgeschlagen: {e}")
        return []


def _add_hardware_entry(brand: str, model: str, hw_type: str):
    from db import get_connection
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO hardware (brand, model, type, ps5_native, notes) "
                    "VALUES (%s, %s, %s, 1, 'Manuell eingetragen via Onboarding')",
                    (brand, model, hw_type)
                )
                conn.commit()
                return cur.lastrowid
    except Exception as e:
        log.error(f"Hardware-Anlage fehlgeschlagen: {e}")
        return None


def _save_driver_onboarding(driver_id: int, data: dict):
    from db import get_connection
    fields = {k: v for k, v in data.items() if v is not None}
    if not fields:
        return
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                set_clause = ", ".join(f"`{k}` = %s" for k in fields)
                cur.execute(
                    f"UPDATE drivers SET {set_clause} WHERE driver_id = %s",
                    list(fields.values()) + [driver_id]
                )
                conn.commit()
    except Exception as e:
        log.error(f"Onboarding-Speicherung fehlgeschlagen: {e}")


async def _get_driver_discord_id_from_db(driver_id: int):
    from db import get_connection
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT discord_id FROM drivers WHERE driver_id = %s", (driver_id,)
                )
                row = cur.fetchone()
                return str(row["discord_id"]) if row and row.get("discord_id") else None
    except Exception:
        return None


# Onboarding State

class OnboardingState:
    def __init__(self, driver_id: int, discord_name: str):
        self.driver_id       = driver_id
        self.discord_name    = discord_name
        self.psn_name        = None
        self.gt7_name        = None
        self.start_number    = None
        self.hardware_type   = None
        self.wheel_base_id   = None
        self.wheel_pedals_id = None
        self.uses_vr         = None


# Schritt 1: PSN-Name

class PsnModal(discord.ui.Modal, title="PSN-Name"):
    psn = discord.ui.TextInput(
        label="Dein PSN-Name",
        placeholder="z.B. Rennfahrer_Max",
        min_length=3, max_length=50
    )

    def __init__(self, state, news_message):
        super().__init__()
        self.state = state
        self.news_message = news_message

    async def on_submit(self, interaction: discord.Interaction):
        self.state.psn_name = self.psn.value.strip()
        await interaction.response.edit_message(
            content=(
                f"PSN-Name gespeichert: **{self.state.psn_name}**\n\n"
                f"Bitte gib jetzt Deinen Nickname aus Gran Turismo 7 ein."
            ),
            view=Gt7NickView(self.state, self.news_message)
        )


class PsnView(discord.ui.View):
    def __init__(self, state, news_message):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message

    @discord.ui.button(label="PSN-Name eingeben", style=discord.ButtonStyle.primary)
    async def enter_psn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PsnModal(self.state, self.news_message))


# Schritt 2: GT7-Nick

class Gt7Modal(discord.ui.Modal, title="GT7-Nickname"):
    gt7 = discord.ui.TextInput(
        label="Dein Nickname in Gran Turismo 7",
        placeholder="z.B. MaxRacer_GT",
        min_length=2, max_length=50
    )

    def __init__(self, state, news_message):
        super().__init__()
        self.state = state
        self.news_message = news_message

    async def on_submit(self, interaction: discord.Interaction):
        self.state.gt7_name = self.gt7.value.strip()
        await interaction.response.edit_message(
            content=(
                f"GT7-Nick gespeichert: **{self.state.gt7_name}**\n\n"
                f"Moechtest Du eine Startnummer (1-999) reservieren? "
                f"Du kannst diesen Schritt auch ueberspringen."
            ),
            view=StartNumberView(self.state, self.news_message)
        )


class Gt7NickView(discord.ui.View):
    def __init__(self, state, news_message):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message

    @discord.ui.button(label="GT7-Nick eingeben", style=discord.ButtonStyle.primary)
    async def enter_gt7(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(Gt7Modal(self.state, self.news_message))


# Schritt 3: Startnummer

class StartNumberModal(discord.ui.Modal, title="Startnummer"):
    number = discord.ui.TextInput(
        label="Deine Startnummer (1-999)",
        placeholder="z.B. 42",
        min_length=1, max_length=3
    )

    def __init__(self, state, news_message):
        super().__init__()
        self.state = state
        self.news_message = news_message

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.number.value.strip()
        if not raw.isdigit() or not (1 <= int(raw) <= 999):
            await interaction.response.edit_message(
                content=(
                    "Ungueltige Eingabe. Bitte gib eine Zahl zwischen 1 und 999 ein.\n\n"
                    "Moechtest Du eine Startnummer reservieren?"
                ),
                view=StartNumberView(self.state, self.news_message)
            )
            return
        num = int(raw)
        taken = _get_taken_start_numbers()
        if num in taken:
            await interaction.response.edit_message(
                content=(
                    f"Die Nummer **{num}** ist bereits vergeben. "
                    f"Bitte waehle eine andere Nummer.\n\n"
                    f"Moechtest Du eine Startnummer reservieren?"
                ),
                view=StartNumberView(self.state, self.news_message)
            )
            return
        self.state.start_number = num
        await interaction.response.edit_message(
            content=f"Startnummer **{num}** reserviert!\n\nWelche Hardware nutzt Du?",
            view=HardwareTypeView(self.state, self.news_message)
        )


class StartNumberView(discord.ui.View):
    def __init__(self, state, news_message):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message

    @discord.ui.button(label="Startnummer eingeben", style=discord.ButtonStyle.primary)
    async def enter_number(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(StartNumberModal(self.state, self.news_message))

    @discord.ui.button(label="Ueberspringen", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="Welche Hardware nutzt Du?",
            view=HardwareTypeView(self.state, self.news_message)
        )


# Schritt 4a: Hardware-Typ

class HardwareTypeView(discord.ui.View):
    def __init__(self, state, news_message):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message

    @discord.ui.button(label="Controller", style=discord.ButtonStyle.secondary)
    async def use_controller(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.state.hardware_type = "controller"
        await interaction.response.edit_message(
            content="Controller ausgewaehlt.\n\nNutzt Du VR?",
            view=VrView(self.state, self.news_message)
        )

    @discord.ui.button(label="Lenkrad", style=discord.ButtonStyle.primary)
    async def use_wheel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.state.hardware_type = "wheel"
        bases = _get_hardware_list("base")
        brands = sorted(set(b["brand"] for b in bases))
        await interaction.response.edit_message(
            content="Lenkrad ausgewaehlt.\n\nBitte waehle den Hersteller Deiner Wheel-Base:",
            view=BaseBrandView(self.state, self.news_message, bases, brands)
        )

    @discord.ui.button(label="Ueberspringen", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="Nutzt Du VR?",
            view=VrView(self.state, self.news_message)
        )


# Schritt 4b: Wheel Base

class BaseBrandView(discord.ui.View):
    def __init__(self, state, news_message, bases, brands):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message
        self.bases = bases
        self.add_item(BaseBrandSelect(state, news_message, bases, brands))

    @discord.ui.button(label="Ueberspringen", style=discord.ButtonStyle.secondary, row=1)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        pedals = _get_hardware_list("pedals")
        brands = sorted(set(p["brand"] for p in pedals))
        await interaction.response.edit_message(
            content="Bitte waehle den Hersteller Deiner Pedale:",
            view=PedalsBrandView(self.state, self.news_message, pedals, brands)
        )


class BaseBrandSelect(discord.ui.Select):
    def __init__(self, state, news_message, bases, brands):
        self.state = state
        self.news_message = news_message
        self.bases = bases
        options = [discord.SelectOption(label=b, value=b) for b in brands]
        options.append(discord.SelectOption(label="Andere", value="__andere__"))
        super().__init__(placeholder="Hersteller waehlen...", options=options)

    async def callback(self, interaction: discord.Interaction):
        brand = self.values[0]
        if brand == "__andere__":
            await interaction.response.send_modal(
                CustomHardwareModal(self.state, self.news_message, "base")
            )
            return
        models = [b for b in self.bases if b["brand"] == brand]
        await interaction.response.edit_message(
            content=f"Hersteller: **{brand}**\n\nBitte waehle Deine Wheel-Base:",
            view=BaseModelView(self.state, self.news_message, brand, models)
        )


class BaseModelView(discord.ui.View):
    def __init__(self, state, news_message, brand, models):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message
        self.brand = brand
        self.add_item(BaseModelSelect(state, news_message, models))

    @discord.ui.button(label="Zurueck", style=discord.ButtonStyle.secondary, row=1)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        bases = _get_hardware_list("base")
        brands = sorted(set(b["brand"] for b in bases))
        await interaction.response.edit_message(
            content="Bitte waehle den Hersteller Deiner Wheel-Base:",
            view=BaseBrandView(self.state, self.news_message, bases, brands)
        )


class BaseModelSelect(discord.ui.Select):
    def __init__(self, state, news_message, models):
        self.state = state
        self.news_message = news_message
        options = [
            discord.SelectOption(label=m["model"], value=str(m["hardware_id"]))
            for m in models
        ]
        super().__init__(placeholder="Modell waehlen...", options=options)

    async def callback(self, interaction: discord.Interaction):
        self.state.wheel_base_id = int(self.values[0])
        pedals = _get_hardware_list("pedals")
        brands = sorted(set(p["brand"] for p in pedals))
        await interaction.response.edit_message(
            content="Wheel-Base gespeichert.\n\nBitte waehle den Hersteller Deiner Pedale:",
            view=PedalsBrandView(self.state, self.news_message, pedals, brands)
        )


class CustomHardwareModal(discord.ui.Modal, title="Andere Hardware"):
    brand = discord.ui.TextInput(label="Hersteller", placeholder="z.B. Heusinkveld", max_length=50)
    model = discord.ui.TextInput(label="Modell", placeholder="z.B. Sprint+", max_length=100)

    def __init__(self, state, news_message, hw_type: str):
        super().__init__()
        self.state = state
        self.news_message = news_message
        self.hw_type = hw_type

    async def on_submit(self, interaction: discord.Interaction):
        brand_val = self.brand.value.strip()
        model_val = self.model.value.strip()
        hw_id = _add_hardware_entry(brand_val, model_val, self.hw_type)

        if self.hw_type == "base":
            self.state.wheel_base_id = hw_id
            pedals = _get_hardware_list("pedals")
            brands = sorted(set(p["brand"] for p in pedals))
            await interaction.response.edit_message(
                content=f"Wheel-Base eingetragen: **{brand_val} {model_val}**\n\n"
                        f"Bitte waehle den Hersteller Deiner Pedale:",
                view=PedalsBrandView(self.state, self.news_message, pedals, brands)
            )
        else:
            self.state.wheel_pedals_id = hw_id
            await interaction.response.edit_message(
                content=f"Pedale eingetragen: **{brand_val} {model_val}**\n\nNutzt Du VR?",
                view=VrView(self.state, self.news_message)
            )


# Schritt 4c: Pedale

class PedalsBrandView(discord.ui.View):
    def __init__(self, state, news_message, pedals, brands):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message
        self.pedals = pedals
        self.add_item(PedalsBrandSelect(state, news_message, pedals, brands))

    @discord.ui.button(label="Ueberspringen", style=discord.ButtonStyle.secondary, row=1)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="Nutzt Du VR?",
            view=VrView(self.state, self.news_message)
        )


class PedalsBrandSelect(discord.ui.Select):
    def __init__(self, state, news_message, pedals, brands):
        self.state = state
        self.news_message = news_message
        self.pedals = pedals
        options = [discord.SelectOption(label=b, value=b) for b in brands]
        options.append(discord.SelectOption(label="Andere", value="__andere__"))
        super().__init__(placeholder="Hersteller waehlen...", options=options)

    async def callback(self, interaction: discord.Interaction):
        brand = self.values[0]
        if brand == "__andere__":
            await interaction.response.send_modal(
                CustomHardwareModal(self.state, self.news_message, "pedals")
            )
            return
        models = [p for p in self.pedals if p["brand"] == brand]
        await interaction.response.edit_message(
            content=f"Hersteller: **{brand}**\n\nBitte waehle Deine Pedale:",
            view=PedalsModelView(self.state, self.news_message, models)
        )


class PedalsModelView(discord.ui.View):
    def __init__(self, state, news_message, models):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message
        self.add_item(PedalsModelSelect(state, news_message, models))

    @discord.ui.button(label="Zurueck", style=discord.ButtonStyle.secondary, row=1)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        pedals = _get_hardware_list("pedals")
        brands = sorted(set(p["brand"] for p in pedals))
        await interaction.response.edit_message(
            content="Bitte waehle den Hersteller Deiner Pedale:",
            view=PedalsBrandView(self.state, self.news_message, pedals, brands)
        )


class PedalsModelSelect(discord.ui.Select):
    def __init__(self, state, news_message, models):
        self.state = state
        self.news_message = news_message
        options = [
            discord.SelectOption(label=m["model"], value=str(m["hardware_id"]))
            for m in models
        ]
        super().__init__(placeholder="Modell waehlen...", options=options)

    async def callback(self, interaction: discord.Interaction):
        self.state.wheel_pedals_id = int(self.values[0])
        await interaction.response.edit_message(
            content="Pedale gespeichert.\n\nNutzt Du VR?",
            view=VrView(self.state, self.news_message)
        )


# Schritt 5: VR

class VrView(discord.ui.View):
    def __init__(self, state, news_message):
        super().__init__(timeout=None)
        self.state = state
        self.news_message = news_message

    @discord.ui.button(label="Ja, ich nutze VR", style=discord.ButtonStyle.success)
    async def yes_vr(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.state.uses_vr = True
        await _finish_onboarding(interaction, self.state)

    @discord.ui.button(label="Nein", style=discord.ButtonStyle.secondary)
    async def no_vr(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.state.uses_vr = False
        await _finish_onboarding(interaction, self.state)

    @discord.ui.button(label="Ueberspringen", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _finish_onboarding(interaction, self.state)


# Abschluss

async def _finish_onboarding(interaction: discord.Interaction, state: OnboardingState):
    _save_driver_onboarding(state.driver_id, {
        "psn_name":        state.psn_name,
        "gt7_name":        state.gt7_name,
        "start_number":    state.start_number,
        "hardware_type":   state.hardware_type,
        "wheel_base_id":   state.wheel_base_id,
        "wheel_pedals_id": state.wheel_pedals_id,
        "uses_vr":         int(state.uses_vr) if state.uses_vr is not None else None,
    })

    lines = ["Alles gespeichert! Viel Spass bei Deinem ersten Rennen in der RTC. 🏁\n"]
    if state.psn_name:      lines.append(f"**PSN:** {state.psn_name}")
    if state.gt7_name:      lines.append(f"**GT7-Nick:** {state.gt7_name}")
    if state.start_number:  lines.append(f"**Startnummer:** #{state.start_number}")
    if state.hardware_type == "controller": lines.append("**Hardware:** Controller")
    elif state.hardware_type == "wheel":    lines.append("**Hardware:** Lenkrad")
    if state.uses_vr is True:  lines.append("**VR:** Ja")
    elif state.uses_vr is False: lines.append("**VR:** Nein")

    await interaction.response.edit_message(content="\n".join(lines), view=None)

    try:
        chan_log = interaction.client.get_channel(int(os.environ.get("CHAN_LOG", 0)))
        if chan_log:
            orga_lines = [f"Onboarding abgeschlossen: **{state.discord_name}**"]
            if state.psn_name:     orga_lines.append(f"  PSN: {state.psn_name}")
            if state.gt7_name:     orga_lines.append(f"  GT7: {state.gt7_name}")
            if state.start_number: orga_lines.append(f"  Startnummer: #{state.start_number}")
            await chan_log.send("\n".join(orga_lines))
    except Exception as e:
        log.error(f"Orga-Benachrichtigung fehlgeschlagen: {e}")

    log.info(f"Onboarding abgeschlossen fuer driver_id={state.driver_id}")


# CHAN_LOG Willkommens-Button

class WelcomeView(discord.ui.View):
    def __init__(self, driver_id: int, discord_id: str, discord_name: str):
        super().__init__(timeout=None)
        self.driver_id    = driver_id
        self.discord_id   = discord_id
        self.discord_name = discord_name

    @discord.ui.button(
        label="Willkommen - Registrierung starten",
        style=discord.ButtonStyle.success,
        custom_id="welcome_onboarding"
    )
    async def start_onboarding(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != str(self.discord_id):
            await interaction.response.send_message(
                "Dieser Button ist nicht fuer Dich.", ephemeral=True
            )
            return

        try:
            await interaction.message.delete()
        except Exception:
            pass

        state = OnboardingState(self.driver_id, self.discord_name)
        await interaction.response.send_message(
            f"Hallo {interaction.user.mention}, willkommen in der RTC!\n\n"
            f"Vor dem ersten Rennen benoetigen wir noch ein paar Angaben von Dir. "
            f"Fangen wir mit Deinem PSN-Namen an:",
            view=PsnView(state, None),
            ephemeral=True
        )


async def _post_welcome_message(bot, driver_id: int, discord_id: str, discord_name: str):
    try:
        chan_log = bot.get_channel(int(os.environ.get("CHAN_LOG", 0)))
        if not chan_log:
            log.error("CHAN_LOG nicht gefunden.")
            return
        mention = f"<@{discord_id}>"
        await chan_log.send(
            f"{mention} Du hast Dich erstmals zu einem Rennen in der RTC angemeldet. "
            f"Falls Du schon hier gefahren bist, melde Dich bitte per Ticket bei der Orga. "
            f"Anderenfalls klicke bitte auf den folgenden Button:",
            view=WelcomeView(driver_id, discord_id, discord_name)
        )
        log.info(f"Willkommensnachricht fuer {discord_name} ({discord_id}) gepostet.")
    except Exception as e:
        log.error(f"Willkommensnachricht fehlgeschlagen: {e}")


# Haupt-Resolver

def resolve_driver(discord_id: str, nickname: str, orga_notify_fn=None, bot=None):
    # 1. DB: Discord-ID
    driver = get_driver_by_discord_id(discord_id)
    if driver:
        log.debug(f"Fahrer per Discord-ID gefunden: {driver['psn_name']}")
        return driver

    # 2. DB: Nickname
    driver = get_driver_by_nickname(nickname)
    if driver:
        log.info(f"Fahrer per Nickname gefunden: {driver['psn_name']} - trage Discord-ID nach.")
        update_driver_discord_id(driver["driver_id"], discord_id)
        sheet_entry = _find_in_sheet_by_nick(nickname)
        if sheet_entry and not sheet_entry.get("discord_id"):
            _update_sheet_discord_id(sheet_entry["row_index"], discord_id)
        driver["discord_id"] = discord_id
        return driver

    # 2b. Sync + nochmal suchen
    try:
        from driver_sync import sync_drivers
        log.info(f"Fahrer '{nickname}' nicht in DB - fuehre driver_sync aus.")
        sync_drivers()
    except Exception as e:
        log.error(f"driver_sync fehlgeschlagen: {e}")

    driver = get_driver_by_discord_id(discord_id)
    if driver:
        log.info(f"Fahrer nach Sync per Discord-ID gefunden: {driver['psn_name']}")
        return driver

    driver = get_driver_by_nickname(nickname)
    if driver:
        log.info(f"Fahrer nach Sync per Nickname gefunden: {driver['psn_name']}")
        update_driver_discord_id(driver["driver_id"], discord_id)
        driver["discord_id"] = discord_id
        return driver

    # 3. Sheet: Nickname (Fallback)
    sheet_entry = _find_in_sheet_by_nick(nickname)
    if sheet_entry:
        psn_name = sheet_entry["psn_name"]
        log.info(f"Fahrer im Sheet gefunden: {psn_name} - lege in DB an.")
        driver_id = create_driver(discord_id, nickname)
        if not sheet_entry.get("discord_id"):
            _update_sheet_discord_id(sheet_entry["row_index"], discord_id)
        driver = get_driver_by_discord_id(discord_id)
        if bot and driver:
            asyncio.create_task(
                _post_welcome_message(bot, driver["driver_id"], discord_id, nickname)
            )
        return driver

    # 4. Komplett neu anlegen
    log.warning(f"Neuer unbekannter Fahrer: {nickname} ({discord_id}) - lege automatisch an.")
    driver_id = create_driver(discord_id, nickname)
    _add_driver_to_sheet(discord_id, nickname)
    driver = get_driver_by_discord_id(discord_id)

    if orga_notify_fn:
        orga_notify_fn(
            f"Neuer Fahrer **{nickname}** wurde automatisch in DB und Sheet eingetragen "
            f"und in CHAN_LOG kontaktiert, um weitere Daten zu ergaenzen."
        )

    if bot and driver:
        asyncio.create_task(
            _post_welcome_message(bot, driver["driver_id"], discord_id, nickname)
        )

    return driver
