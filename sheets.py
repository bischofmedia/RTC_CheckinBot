"""
RTC CheckinBot – sheets.py
Google Sheets Sync – Anmeldeliste übertragen
"""

import logging
import os
from db import get_all_registrations

log = logging.getLogger("CheckinBot")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_sheet_client():
    """Erstellt einen authentifizierten gspread-Client."""
    import gspread
    from google.oauth2.service_account import Credentials
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_CREDENTIALS_FILE"], scopes=SCOPES
    )
    return gspread.authorize(creds)


def sync_registrations_to_sheet(race_id: int):
    """
    Überträgt die aktuellen Anmeldungen (PSN-Namen) ins Google Sheet.
    Schreibt alle PSN-Namen der aktuell angemeldeten Fahrer in den
    konfigurierten Tab (GOOGLE_SHEETS_TAB), Spalte A ab Zeile 2.
    Löscht vorher alle alten Einträge.

    Im TEST_MODE wird kein Sync durchgeführt.
    """
    test_mode = os.environ.get("TEST_MODE", "false").lower() == "true"
    if test_mode:
        log.info("TEST_MODE aktiv – kein Sheet-Sync.")
        return

    try:
        registrations = get_all_registrations(race_id)
        # Discord-Nicks schreiben (die Tabelle wandelt sie in PSN-Namen um)
        discord_names = [[r["discord_name"] or r["psn_name"]] for r in registrations]

        client = _get_sheet_client()
        sheet = client.open_by_key(os.environ["GOOGLE_SHEETS_ID"])
        ws = sheet.worksheet(os.environ.get("GOOGLE_SHEETS_TAB", "Anmeldungen"))

        # Apollo-Grabber Format: Fahrerliste in Q1 als newline-getrennter String
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now_str = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M")
        driver_list = "\n".join(r[0] for r in discord_names)

        ws.update(range_name="B1", values=[[f"Letzte Änderung:\n{now_str} Uhr"]], value_input_option="USER_ENTERED")
        ws.update(range_name="Q1", values=[[driver_list]], value_input_option="USER_ENTERED")
        # D1: Anzahl der Grids
        from checkin_bot import calculate_grids
        grid_count = calculate_grids(len(discord_names))
        ws.update(range_name="D1", values=[[grid_count]], value_input_option="USER_ENTERED")

        from datetime import datetime
        from zoneinfo import ZoneInfo
        from db import save_state_value
        import subprocess
        sync_ts = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M")
        save_state_value("last_sheet_sync", sync_ts)
        log.info(f"Sheet-Sync: {len(discord_names)} Fahrer übertragen.")

        # Grid-DB-Sync (wie Apollo Grabber)
        subprocess.Popen(
            ["python3", "/home/ubuntu/RTC_CheckinBot/sync_grid_to_db.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info("Grid-DB-Sync gestartet.")

    except Exception as e:
        log.error(f"Sheet-Sync fehlgeschlagen: {e}")


def clear_lobby_codes_sheet():
    """
    Löscht alle Lobby-Codes aus dem Sheet (Tab LobbyCodes, Spalten A-C ab Zeile 2).
    Im TEST_MODE wird nichts gelöscht.
    """
    test_mode = os.environ.get("TEST_MODE", "false").lower() == "true"
    if test_mode:
        log.info("TEST_MODE aktiv – kein Lobby-Code-Sheet-Clear.")
        return

    try:
        client = _get_sheet_client()
        sheet = client.open_by_key(os.environ["GOOGLE_SHEETS_ID"])
        ws = sheet.worksheet("LobbyCodes")
        ws.batch_clear(["A2:C200"])
        log.info("Lobby-Codes im Sheet gelöscht.")

    except Exception as e:
        log.error(f"Lobby-Code-Sheet-Clear fehlgeschlagen: {e}")
