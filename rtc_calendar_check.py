#!/usr/bin/env python3
# ============================================================
# RTC CheckinBot – Kalender-Test
# Liest das nächste Rennen aus der DB und gibt den Event-Text aus
# ============================================================

import os
import pymysql
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- DB-Verbindung ---
def get_connection():
    return pymysql.connect(
        host     = os.getenv("DB_HOST"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        database = os.getenv("DB_NAME"),
        charset  = "utf8mb4",
        cursorclass = pymysql.cursors.DictCursor
    )

# --- Nächstes Rennen ermitteln ---
# Logik: Rennen ist immer Montag 20:45 Uhr.
# Der Bot läuft dienstags um 10:00 Uhr.
# "Nächstes Rennen" = der kommende Montag (in 6 Tagen).
def get_next_race():
    today = date.today()  # Dienstag
    next_monday = today + timedelta(days=6)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT race_number, race_date, track_name, laps, time_of_day, weather_code, is_pause
                FROM race_calendar
                WHERE season_id = %s
                  AND race_date = %s
                LIMIT 1
            """, (os.getenv("CURRENT_SEASON_ID", 16), next_monday))
            return cur.fetchone()

# --- Ausgabe ---
def format_message(race):
    if race is None:
        return "⚠️  Kein Eintrag im Kalender für nächsten Montag gefunden."

    if race["is_pause"]:
        return "😴 Nächste Woche kein Rennen – Pause!"

    return (
        f"🏁 Nächstes Rennen – Rennen {race['race_number']}\n"
        f"📅 Datum:      {race['race_date'].strftime('%d.%m.%Y')} · 20:45 Uhr\n"
        f"🗺️  Strecke:    {race['track_name']}\n"
        f"🔄 Runden:     {race['laps']}\n"
        f"🌤️  Tageszeit:  {race['time_of_day']}\n"
        f"🌦️  Wetter:     {race['weather_code']}"
    )

if __name__ == "__main__":
    race = get_next_race()
    print(format_message(race))
