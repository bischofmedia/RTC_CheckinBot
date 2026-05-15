"""
driver_sync.py – Synchronisation der Fahrerdaten zwischen Google Sheets (DB_drvr) und MariaDB.

Regeln:
  - Sheet hat IMMER Vorrang: Ist ein Feld im Sheet befüllt, wird der DB-Wert überschrieben.
  - Ist ein Sheet-Feld leer, wird es aus der DB befüllt (sofern dort ein Wert vorhanden ist).
  - current_rating wird IMMER aus dem Sheet übernommen (kein Vorrang-Check).
  - Jeder Fahrer im Sheet bekommt is_active = 1 in der DB.
  - Fahrer im Sheet, die nicht in der DB gefunden werden, werden neu angelegt.

Match-Reihenfolge pro Sheet-Zeile:
  1. discord_id
  2. psn_name
  3. discord_name
  4. gt7_name

Einmalige DB-Migration (einmal manuell ausführen, NICHT Teil dieses Scripts):
  ALTER TABLE drivers CHANGE initial_rating current_rating DECIMAL(6,2) NULL DEFAULT NULL;

Aufruf:
  Manuell:    python driver_sync.py
  Per Import: from driver_sync import sync_drivers
"""

import os
import re
import logging
from logging.handlers import RotatingFileHandler

import pymysql
import pymysql.cursors
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger("driver_sync")
logger.setLevel(logging.INFO)

_handler = RotatingFileHandler("driver_sync.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_handler)

_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_console)

# ── Konfiguration ─────────────────────────────────────────────────────────────
GOOGLE_SHEETS_ID       = os.getenv("GOOGLE_SHEETS_ID")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
SHEET_TAB              = "DB_drvr"

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Sheet-Spalten (0-basiert)
COL_NR           = 1   # B  – laufende Nummer
COL_PSN_NAME     = 2   # C  – PSN-Name
COL_RANKING      = 3   # D  – akt. Ranking  (z.B. "104,49%")
COL_NAT          = 4   # E  – Nationalität (Flaggen-Emoji)
COL_START_NUMBER = 6   # G  – Startnummer
COL_DISCORD_NAME = 10  # K  – Discord-Name/Nick

# Die letzten drei Spalten (Indizes werden dynamisch ermittelt)
# Reihenfolge: ..., PSN-Name (Referenz), GT7-Name, Discord-ID
COL_OFFSET_PSN_REF   = -3  # vorletzte-2 (nicht genutzt für Sync, nur Referenz)
COL_OFFSET_GT7_NAME  = -2  # vorletzte
COL_OFFSET_DISCORD_ID = -1  # letzte

HEADER_ROW = 6  # 0-basiert (= Zeile 7 im Sheet)
DATA_START_ROW = 7  # 0-basiert (= Zeile 8 im Sheet)


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def flag_emoji_to_country_code(emoji: str) -> str | None:
    """Konvertiert ein Flaggen-Emoji (z.B. 🇩🇪) in einen 2-Buchstaben-Ländercode (DE)."""
    if not emoji or len(emoji) < 2:
        return None
    try:
        chars = list(emoji.strip())
        # Flaggen-Emojis bestehen aus zwei Regional Indicator Symbols (U+1F1E6–U+1F1FF)
        if len(chars) >= 2 and 0x1F1E6 <= ord(chars[0]) <= 0x1F1FF:
            code = "".join(chr(ord(c) - 0x1F1E6 + ord("A")) for c in chars[:2])
            return code if len(code) == 2 else None
    except Exception:
        pass
    return None


def parse_rating(value: str) -> float | None:
    """Konvertiert '104,49%' → 104.49 (float) oder None bei leerem/ungültigem Wert."""
    if not value or not value.strip():
        return None
    cleaned = value.strip().rstrip("%").replace(",", ".").strip()
    # Ungültige Werte wie "852,01%" (Crash-Marker) trotzdem übernehmen
    try:
        return float(cleaned)
    except ValueError:
        return None


def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        autocommit=False,
    )


def get_sheet_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


# ── Sheet-Daten einlesen ──────────────────────────────────────────────────────

def read_sheet_drivers(worksheet) -> list[dict]:
    """Liest alle Fahrerzeilen aus dem Sheet und gibt sie als strukturierte Liste zurück."""
    all_rows = worksheet.get_all_values()
    drivers = []

    for row_idx, row in enumerate(all_rows):
        if row_idx <= HEADER_ROW:
            continue  # Header und Kommentarzeilen überspringen

        # Zeile leer oder keine PSN-Name-Zelle → Ende der Fahrerliste
        psn_name = row[COL_PSN_NAME].strip() if len(row) > COL_PSN_NAME else ""
        if not psn_name:
            continue

        # Letzte drei Spalten dynamisch auslesen
        gt7_name   = row[COL_OFFSET_GT7_NAME].strip()   if len(row) >= 2 else ""
        discord_id = row[COL_OFFSET_DISCORD_ID].strip() if len(row) >= 1 else ""

        ranking_raw  = row[COL_RANKING].strip()      if len(row) > COL_RANKING      else ""
        nat_raw      = row[COL_NAT].strip()           if len(row) > COL_NAT          else ""
        start_number = row[COL_START_NUMBER].strip()  if len(row) > COL_START_NUMBER else ""
        discord_name = row[COL_DISCORD_NAME].strip()  if len(row) > COL_DISCORD_NAME else ""

        drivers.append({
            "_row_idx":     row_idx,          # 0-basierter Index für späteres Sheet-Update
            "_row":         row,              # Gesamte Zeile für Rückschreiben
            "psn_name":     psn_name,
            "discord_id":   discord_id or None,
            "discord_name": discord_name or None,
            "gt7_name":     gt7_name or None,
            "nat":          flag_emoji_to_country_code(nat_raw),
            "start_number": int(start_number) if start_number.isdigit() else None,
            "current_rating": parse_rating(ranking_raw),
        })

    logger.info(f"Sheet: {len(drivers)} Fahrerzeilen eingelesen.")
    return drivers


# ── DB-Daten einlesen ─────────────────────────────────────────────────────────

def read_db_drivers(cursor) -> dict:
    """
    Gibt ein Dict zurück mit Lookup-Dicts für schnelle Suche:
    {
        'by_discord_id':   { discord_id:   driver_row },
        'by_psn_name':     { psn_name:     driver_row },
        'by_discord_name': { discord_name: driver_row },
        'by_gt7_name':     { gt7_name:     driver_row },
    }
    """
    cursor.execute("SELECT * FROM drivers")
    rows = cursor.fetchall()

    index = {
        "by_discord_id":   {},
        "by_psn_name":     {},
        "by_discord_name": {},
        "by_gt7_name":     {},
    }

    for row in rows:
        if row.get("discord_id"):
            index["by_discord_id"][str(row["discord_id"]).strip()] = row
        if row.get("psn_name"):
            index["by_psn_name"][row["psn_name"].strip().lower()] = row
        if row.get("discord_name"):
            index["by_discord_name"][row["discord_name"].strip().lower()] = row
        if row.get("gt7_name"):
            index["by_gt7_name"][row["gt7_name"].strip().lower()] = row

    logger.info(f"DB: {len(rows)} Fahrer eingelesen.")
    return index


# ── Match-Logik ───────────────────────────────────────────────────────────────

def find_db_match(sheet_driver: dict, db_index: dict) -> dict | None:
    """
    Sucht einen DB-Eintrag für einen Sheet-Fahrer.
    Priorität: discord_id → psn_name → discord_name → gt7_name
    """
    if sheet_driver.get("discord_id"):
        match = db_index["by_discord_id"].get(str(sheet_driver["discord_id"]).strip())
        if match:
            return match

    if sheet_driver.get("psn_name"):
        match = db_index["by_psn_name"].get(sheet_driver["psn_name"].strip().lower())
        if match:
            return match

    if sheet_driver.get("discord_name"):
        match = db_index["by_discord_name"].get(sheet_driver["discord_name"].strip().lower())
        if match:
            return match

    if sheet_driver.get("gt7_name"):
        match = db_index["by_gt7_name"].get(sheet_driver["gt7_name"].strip().lower())
        if match:
            return match

    return None


# ── Sync-Logik ────────────────────────────────────────────────────────────────

SYNC_FIELDS = ["psn_name", "discord_id", "discord_name", "gt7_name", "nat", "start_number"]
# current_rating wird separat behandelt (immer aus Sheet übernehmen)


def merge_driver(sheet: dict, db: dict) -> tuple[dict, dict, list[str]]:
    """
    Berechnet die Updates für DB und Sheet.

    Rückgabe:
      db_updates   – Dict mit Feldern die in der DB zu aktualisieren sind
      sheet_updates – Dict mit Feldern die ins Sheet zurückzuschreiben sind (nur wenn Sheet leer war)
      change_log   – Liste mit menschenlesbaren Änderungsbeschreibungen
    """
    db_updates = {}
    sheet_updates = {}
    change_log = []

    for field in SYNC_FIELDS:
        sheet_val = sheet.get(field)
        db_val    = db.get(field)

        if sheet_val not in (None, ""):
            # Sheet hat Wert → DB überschreiben wenn abweichend
            if str(db_val or "").strip() != str(sheet_val).strip():
                db_updates[field] = sheet_val
                change_log.append(f"  DB.{field}: '{db_val}' → '{sheet_val}' (Sheet-Vorrang)")
        else:
            # Sheet leer → DB-Wert ins Sheet übernehmen
            if db_val not in (None, ""):
                sheet_updates[field] = db_val
                change_log.append(f"  Sheet.{field}: '' → '{db_val}' (aus DB)")

    # Rating immer aus Sheet übernehmen
    if sheet.get("current_rating") is not None:
        if db.get("current_rating") != sheet["current_rating"]:
            db_updates["current_rating"] = sheet["current_rating"]
            change_log.append(
                f"  DB.current_rating: '{db.get('current_rating')}' → '{sheet['current_rating']}' (immer Sheet)"
            )

    # is_active immer setzen
    if not db.get("is_active"):
        db_updates["is_active"] = 1
        change_log.append("  DB.is_active: 0 → 1")

    return db_updates, sheet_updates, change_log


# ── Sheet-Update zurückschreiben ──────────────────────────────────────────────

# Mapping Feldname → Spaltenindex im Sheet (für Rückschreiben)
FIELD_TO_COL = {
    "psn_name":     COL_PSN_NAME,
    "discord_name": COL_DISCORD_NAME,
    "nat":          COL_NAT,
    "start_number": COL_START_NUMBER,
    # gt7_name und discord_id stehen in den letzten Spalten → dynamisch
}


def apply_sheet_updates(worksheet, row_idx: int, row: list, sheet_updates: dict):
    """Schreibt fehlende Felder aus der DB zurück ins Sheet (nur leere Zellen)."""
    total_cols = len(row)

    for field, value in sheet_updates.items():
        if field in FIELD_TO_COL:
            col_idx = FIELD_TO_COL[field]
        elif field == "gt7_name":
            col_idx = total_cols + COL_OFFSET_GT7_NAME
        elif field == "discord_id":
            col_idx = total_cols + COL_OFFSET_DISCORD_ID
        else:
            continue

        # gspread: 1-basiert, Zeile = row_idx + 1
        cell = gspread.utils.rowcol_to_a1(row_idx + 1, col_idx + 1)
        worksheet.update_acell(cell, str(value))


# ── Haupt-Sync-Funktion ───────────────────────────────────────────────────────

def sync_drivers() -> dict:
    """
    Führt den vollständigen Abgleich durch.

    Rückgabe: {
        'matched':    int,  – Fahrer gefunden und abgeglichen
        'new_in_db':  int,  – Fahrer neu in DB angelegt
        'db_updates': int,  – Anzahl DB-Felder aktualisiert
        'sheet_back': int,  – Anzahl Sheet-Zellen zurückgeschrieben
    }
    """
    logger.info("=" * 60)
    logger.info("driver_sync gestartet")

    stats = {"matched": 0, "new_in_db": 0, "db_updates": 0, "sheet_back": 0}

    # Verbindungen aufbauen
    gc        = get_sheet_client()
    workbook  = gc.open_by_key(GOOGLE_SHEETS_ID)
    worksheet = workbook.worksheet(SHEET_TAB)

    conn   = get_db_connection()
    cursor = conn.cursor()

    try:
        sheet_drivers = read_sheet_drivers(worksheet)
        db_index      = read_db_drivers(cursor)

        db_batch_updates  = []  # (driver_id, update_dict)
        sheet_batch_writes = []  # (row_idx, row, sheet_updates)
        new_drivers       = []  # sheet_driver dicts die neu angelegt werden

        for sheet_driver in sheet_drivers:
            db_match = find_db_match(sheet_driver, db_index)

            if db_match:
                stats["matched"] += 1
                db_updates, sheet_updates, change_log = merge_driver(sheet_driver, db_match)

                if db_updates:
                    db_batch_updates.append((db_match["driver_id"], db_updates))
                    stats["db_updates"] += len(db_updates)

                if sheet_updates:
                    sheet_batch_writes.append(
                        (sheet_driver["_row_idx"], sheet_driver["_row"], sheet_updates)
                    )
                    stats["sheet_back"] += len(sheet_updates)

                if change_log:
                    logger.info(
                        f"MATCH '{sheet_driver['psn_name']}' (driver_id={db_match['driver_id']}):"
                    )
                    for line in change_log:
                        logger.info(line)
            else:
                # Kein Match → neu in DB anlegen
                new_drivers.append(sheet_driver)
                stats["new_in_db"] += 1
                logger.info(f"NEU (nicht in DB): '{sheet_driver['psn_name']}'")

        # ── DB-Updates (Batch) ────────────────────────────────────────────────
        for driver_id, updates in db_batch_updates:
            set_clause = ", ".join(f"`{k}` = %s" for k in updates)
            values     = list(updates.values()) + [driver_id]
            cursor.execute(
                f"UPDATE drivers SET {set_clause} WHERE driver_id = %s",
                values,
            )

        # ── Neue Fahrer in DB anlegen ─────────────────────────────────────────
        for d in new_drivers:
            cursor.execute(
                """
                INSERT INTO drivers
                    (psn_name, discord_id, discord_name, gt7_name, nat,
                     start_number, current_rating, is_active)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, 1)
                """,
                (
                    d["psn_name"],
                    d.get("discord_id"),
                    d.get("discord_name"),
                    d.get("gt7_name"),
                    d.get("nat"),
                    d.get("start_number"),
                    d.get("current_rating"),
                ),
            )
            logger.info(
                f"  → In DB angelegt: psn='{d['psn_name']}', discord_id='{d.get('discord_id')}'"
            )

        conn.commit()
        logger.info(
            f"DB-Commit: {len(db_batch_updates)} Updates, {len(new_drivers)} Neuanlagen."
        )

        # ── Sheet-Rückschreiben (nach DB-Commit, damit kein Rollback nötig) ───
        for row_idx, row, sheet_updates in sheet_batch_writes:
            apply_sheet_updates(worksheet, row_idx, row, sheet_updates)

    except Exception as e:
        conn.rollback()
        logger.error(f"Fehler – Rollback ausgeführt: {e}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info(
        f"Sync abgeschlossen: {stats['matched']} gematcht, "
        f"{stats['new_in_db']} neu in DB, "
        f"{stats['db_updates']} DB-Felder aktualisiert, "
        f"{stats['sheet_back']} Sheet-Zellen zurückgeschrieben."
    )
    return stats


# ── Direktaufruf ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = sync_drivers()
    print("\nErgebnis:")
    print(f"  Gematcht:               {result['matched']}")
    print(f"  Neu in DB angelegt:     {result['new_in_db']}")
    print(f"  DB-Felder aktualisiert: {result['db_updates']}")
    print(f"  Sheet-Zellen gefüllt:   {result['sheet_back']}")
