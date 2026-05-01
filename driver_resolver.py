"""
RTC CheckinBot – driver_resolver.py
Fahrer-Auflösung: Discord-ID → Nickname → Sheet → Neu anlegen
"""

import logging
import os
import gspread
from google.oauth2.service_account import Credentials
from db import (
    get_driver_by_discord_id,
    get_driver_by_nickname,
    update_driver_discord_id,
    create_driver,
)

log = logging.getLogger("CheckinBot")

# Google Sheets Spalten in DB_drvr (0-basiert, Header in Zeile 7)
COL_PSN        = 1   # B – PSN-Name
COL_NICK       = 9   # J – Server-Nickname
COL_DISCORD_ID = 84  # DC – Discord-ID (Spalte 85 = DC)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_sheet_client():
    """Erstellt einen authentifizierten gspread-Client."""
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
    )
    return gspread.authorize(creds)


def _get_drvr_worksheet():
    """Gibt das DB_drvr Worksheet zurück."""
    client = _get_sheet_client()
    sheet = client.open_by_key(os.environ["GOOGLE_SHEETS_ID"])
    return sheet.worksheet("DB_drvr")


def _find_in_sheet_by_nick(nickname: str) -> dict | None:
    """
    Sucht einen Fahrer im Sheet DB_drvr anhand des Server-Nicknamens (Spalte J).
    Gibt ein Dict mit psn_name, discord_id zurück oder None.
    """
    try:
        ws = _get_drvr_worksheet()
        # Alle Daten ab Zeile 8 (Header ist Zeile 7)
        records = ws.get_all_values()
        header_row = 6  # 0-basiert = Zeile 7

        for i, row in enumerate(records):
            if i <= header_row:
                continue
            if len(row) > COL_NICK and row[COL_NICK].strip() == nickname.strip():
                psn = row[COL_PSN].strip() if len(row) > COL_PSN else ""
                discord_id = row[COL_DISCORD_ID].strip() if len(row) > COL_DISCORD_ID else ""
                return {
                    "psn_name": psn or nickname,
                    "discord_id": discord_id,
                    "row_index": i + 1,  # 1-basiert für gspread
                }
        return None
    except Exception as e:
        log.error(f"Sheet-Suche nach Nickname '{nickname}' fehlgeschlagen: {e}")
        return None


def _update_sheet_discord_id(row_index: int, discord_id: str):
    """Trägt die Discord-ID in Spalte DC des Sheets nach."""
    try:
        ws = _get_drvr_worksheet()
        # Spalte DC = Spalte 85 (1-basiert)
        ws.update_cell(row_index, COL_DISCORD_ID + 1, discord_id)
        log.info(f"Discord-ID {discord_id} in Sheet Zeile {row_index} eingetragen.")
    except Exception as e:
        log.error(f"Sheet-Update Discord-ID fehlgeschlagen: {e}")


def resolve_driver(discord_id: str, nickname: str, orga_notify_fn=None) -> dict | None:
    """
    Löst einen Fahrer anhand von Discord-ID und Server-Nickname auf.

    Reihenfolge:
    1. DB-Suche nach Discord-ID
    2. DB-Suche nach Nickname
    3. Sheet-Suche nach Nickname
    4. Neuen Fahrer anlegen

    Gibt das Driver-Dict zurück oder None bei kritischem Fehler.
    orga_notify_fn: optionale async-Funktion die Orga-Meldungen sendet (wird hier nicht awaited).
    """

    # ── 1. DB: Discord-ID ────────────────────────────────────────────────
    driver = get_driver_by_discord_id(discord_id)
    if driver:
        log.debug(f"Fahrer per Discord-ID gefunden: {driver['psn_name']}")
        return driver

    # ── 2. DB: Nickname ──────────────────────────────────────────────────
    driver = get_driver_by_nickname(nickname)
    if driver:
        log.info(f"Fahrer per Nickname gefunden: {driver['psn_name']} – trage Discord-ID nach.")
        update_driver_discord_id(driver["driver_id"], discord_id)
        # Auch im Sheet nachtragen
        sheet_entry = _find_in_sheet_by_nick(nickname)
        if sheet_entry and not sheet_entry.get("discord_id"):
            _update_sheet_discord_id(sheet_entry["row_index"], discord_id)
        driver["discord_id"] = discord_id
        return driver

    # ── 3. Sheet: Nickname ───────────────────────────────────────────────
    sheet_entry = _find_in_sheet_by_nick(nickname)
    if sheet_entry:
        psn_name = sheet_entry["psn_name"]
        log.info(f"Fahrer im Sheet gefunden: {psn_name} – lege in DB an.")
        driver_id = create_driver(discord_id, nickname)
        # Discord-ID im Sheet nachtragen falls fehlend
        if not sheet_entry.get("discord_id"):
            _update_sheet_discord_id(sheet_entry["row_index"], discord_id)
        # Orga benachrichtigen
        if orga_notify_fn:
            orga_notify_fn(
                f"ℹ️ Fahrer **{nickname}** (PSN: {psn_name}) wurde automatisch in der DB angelegt. "
                f"Discord-ID wurde eingetragen."
            )
        return get_driver_by_discord_id(discord_id)

    # ── 4. Komplett neu anlegen ──────────────────────────────────────────
    log.warning(f"Unbekannter Fahrer: {nickname} ({discord_id}) – lege neu an.")
    driver_id = create_driver(discord_id, nickname)
    if orga_notify_fn:
        orga_notify_fn(
            f"⚠️ Neuer unbekannter Fahrer angelegt: **{nickname}** – "
            f"PSN-Name bitte im Sheet DB_drvr nachtragen!"
        )
    return get_driver_by_discord_id(discord_id)


def check_psn_sync(driver: dict) -> bool:
    """
    Prüft ob PSN-Name in Sheet und DB übereinstimmen.
    Bei Abweichung → Orga-Meldung (kein Auto-Update, PSN ist unveränderlich).
    Gibt True zurück wenn alles ok, False bei Abweichung.
    """
    try:
        sheet_entry = _find_in_sheet_by_nick(driver.get("discord_name", ""))
        if not sheet_entry:
            return True  # Nicht im Sheet → kein Vergleich möglich

        sheet_psn = sheet_entry.get("psn_name", "").strip()
        db_psn = driver.get("psn_name", "").strip()

        if sheet_psn and sheet_psn != db_psn:
            log.warning(
                f"PSN-Abweichung: DB='{db_psn}', Sheet='{sheet_psn}' "
                f"für Fahrer {driver.get('discord_name')}"
            )
            return False
        return True
    except Exception as e:
        log.error(f"PSN-Sync-Check fehlgeschlagen: {e}")
        return True
