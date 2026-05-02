"""
RTC CheckinBot – message_builder.py
Baut die Channel-Nachricht und den Status-Button-Text zusammen.
"""

import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from db import (
    get_track_header_stats,
    get_driver_grid_assignment,
    get_driver_overall_stats,
    get_next_monday_race,
    get_next_future_race,
    get_next_monday_is_pause,
    get_all_registrations,
    get_registration_count,
    get_grid_override,
    get_log_entries,
    get_registration,
    has_abo,
    get_driver_track_stats,
    get_track_overall_stats,
    get_driver_current_rating,
    get_driver_season_standings,
    get_active_season_id,
)

log = logging.getLogger("CheckinBot")
BERLIN = ZoneInfo("Europe/Berlin")

DRIVERS_PER_GRID = int(os.environ.get("DRIVERS_PER_GRID", 15))
MAX_GRIDS        = int(os.environ.get("MAX_GRIDS", 4))
LOBBY_OPEN       = os.environ.get("LOBBY_OPEN", "20:30")
TEST_MODE        = os.environ.get("TEST_MODE", "false").lower() == "true"

WEEKDAYS_DE = {
    0: "Mo", 1: "Di", 2: "Mi", 3: "Do", 4: "Fr", 5: "Sa", 6: "So"
}


# ─────────────────────────────────────────────
# Hilfsfunktionen
# ─────────────────────────────────────────────

def _weather_emoji(category: str) -> str:
    if category == "rain":
        return "🌧️"
    if category == "cloudy":
        return "☁️"
    return "☀️"


def _format_date(d) -> str:
    """Formatiert ein date-Objekt als 'Montag, 18.03.2024'."""
    if d is None:
        return "?"
    weekdays = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    return f"{weekdays[d.weekday()]}, {d.strftime('%d.%m.%Y')}"


def _next_tuesday() -> str:
    """Gibt den nächsten Dienstag als formatierten String zurück."""
    today = datetime.now(BERLIN).date()
    days = (1 - today.weekday()) % 7
    if days == 0:
        days = 7
    next_tue = today + timedelta(days=days)
    return next_tue.strftime("%d.%m.%Y")


def _calculate_grids(driver_count: int) -> int:
    """Berechnet die Anzahl der Grids basierend auf der Fahrerzahl."""
    if driver_count == 0:
        return 0
    grids = max(1, driver_count // DRIVERS_PER_GRID)
    return min(grids, MAX_GRIDS)


def get_current_grid_count(race_id: int, driver_count: int, sunday_locked: bool = False) -> int:
    """
    Gibt die aktuelle Grid-Anzahl zurück.
    Priorität: Override > Sunday-Lock > Berechnung
    """
    override = get_grid_override(race_id)
    if override:
        return override["grid_count"]
    return _calculate_grids(driver_count)


def get_status(race_id: int, grid_count: int, driver_count: int) -> tuple[str, str]:
    """
    Gibt (emoji, text) für den aktuellen Anmeldestatus zurück.
    Berücksichtigt: offen, Warteliste, geschlossen (🔴)
    """
    now = datetime.now(BERLIN)
    deadline_str = os.environ.get("REGISTRATION_DEADLINE", "20:45")
    h, m = map(int, deadline_str.split(":"))

    # Montag nach Deadline → 🔴
    if now.weekday() == 0:  # Montag
        deadline = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if now >= deadline:
            return "🔴", "Anmeldung geschlossen"

    max_drivers = grid_count * DRIVERS_PER_GRID
    free_slots = max_drivers - driver_count

    # Sonntag 18:00+ und Grids voll → 🟡
    if now.weekday() == 6 and now.hour >= 18:
        if free_slots <= 0:
            return "🟡", f"Warteliste aktiv · {driver_count} Fahrer · {grid_count} Grids"
        return "🟢", f"Anmeldung offen · {driver_count} Fahrer · {grid_count} Grids · {free_slots} Plätze frei"

    # Normal → 🟢
    return "🟢", f"Anmeldung offen · {driver_count} Fahrer · {grid_count} Grids"


def is_registration_closed() -> bool:
    """Gibt True zurück wenn die Anmeldung geschlossen ist (🔴)."""
    now = datetime.now(BERLIN)
    if now.weekday() != 0:
        return False
    deadline_str = os.environ.get("REGISTRATION_DEADLINE", "20:45")
    h, m = map(int, deadline_str.split(":"))
    deadline = now.replace(hour=h, minute=m, second=0, microsecond=0)
    return now >= deadline


def is_waitlist_active(race_id: int) -> bool:
    """Gibt True zurück wenn die Warteliste aktiv ist."""
    now = datetime.now(BERLIN)
    if not (now.weekday() == 6 and now.hour >= 18) and not (now.weekday() == 0):
        return False
    driver_count = get_registration_count(race_id)
    override = get_grid_override(race_id)
    if override:
        grid_count = override["grid_count"]
    else:
        grid_count = _calculate_grids(driver_count)
    return driver_count >= grid_count * DRIVERS_PER_GRID


# ─────────────────────────────────────────────
# Log-Einträge formatieren
# ─────────────────────────────────────────────

def _format_log_entry(entry: dict) -> str:
    """Formatiert einen Log-Eintrag als Discord-Zeile."""
    ts = entry["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    weekday = WEEKDAYS_DE.get(ts.weekday(), "??")
    time_str = ts.strftime("%H:%M")
    name = entry.get("psn_name") or entry.get("discord_name") or "Unbekannt"
    action = entry["action"]

    if action == "angemeldet":
        return f"{weekday} {time_str} 🟢 {name}"
    elif action == "abgemeldet":
        return f"{weekday} {time_str} 🔴 {name}"
    elif action == "abo_angemeldet":
        # Nur automatische Dienstags-Anmeldungen zeigen
        return f"{weekday} {time_str} 🟢 {name} (Abo)"
    elif action == "abo_abgemeldet":
        return None  # Nicht im Log anzeigen
    elif action == "warteliste":
        return f"{weekday} {time_str} 🟢 → 🟡 {name}"
    elif action == "nachgerueckt":
        return f"{weekday} {time_str} 🟡 → 🟢 {name}"
    elif action == "warteliste_abgemeldet":
        return f"{weekday} {time_str} 🟡 → 🔴 {name}"
    return None


def build_log_section(race_id: int) -> str:
    """Baut den Log-Bereich der Channel-Nachricht als Codeblock."""
    entries = get_log_entries(race_id)
    if not entries:
        return ""
    lines = [_format_log_entry(e) for e in entries]
    lines = [l for l in lines if l is not None]
    if not lines:
        return ""
    return "```\n" + "\n".join(lines) + "\n```"



def build_track_stats_block(race: dict) -> str:
    """Baut den Streckenstatistik-Block für den Channel-Header."""
    track_id = race.get("track_id")
    if not track_id:
        return ""
    try:
        from db import get_track_header_stats
        stats = get_track_header_stats(track_id)
    except Exception:
        return ""

    if stats["total_races"] == 0:
        return "📊 RTC fährt diese Strecke zum ersten Mal!"

    lines = []
    last = f" · zuletzt in {stats['last_season']}" if stats.get("last_season") else ""
    lines.append(f"📊 RTC war hier {stats['total_races']}×{last}")

    if stats["top_winners"]:
        winners = ", ".join(f"{w['psn_name']} ({w['wins']}×)" for w in stats["top_winners"])
        lines.append(f"🏆 Rekord-Sieger: {winners}")

    if stats["top_vehicles"]:
        cars = ", ".join(v["vehicle_name"] for v in stats["top_vehicles"])
        lines.append(f"🚗 Top Fahrzeuge: {cars}")

    if stats.get("record") and stats["record"].get("fastest_lap_time"):
        r = stats["record"]
        gv = f" ({r['game_str']})" if r.get("game_str") else (f" ({r['game_version']})" if r.get("game_version") else "")
        lines.append(f"⏱️ Rekord: {r['fastest_lap_time']} · {r['psn_name']} · {r['season_name']}{gv}")

    return "\n".join(lines)

# ─────────────────────────────────────────────
# Channel-Nachricht
# ─────────────────────────────────────────────

def build_channel_message(race_id: int | None = None, race: dict | None = None) -> tuple[str, bool]:
    """
    Baut die komplette Channel-Nachricht.
    Gibt (message_text, show_buttons) zurück.

    Variante A: Rennen vorhanden
    Variante B: Pause-Woche
    Variante C: Kein zukünftiges Rennen
    """
    test_banner = "⚠️ **TESTMODUS** – keine Übertragung ins Sheet/Grid ⚠️\n" if TEST_MODE else ""

    # ── Variante A: Rennen nächsten Montag ───────────────────────────────
    if race and race_id:
        driver_count = get_registration_count(race_id)
        grid_count = get_current_grid_count(race_id, driver_count)
        status_emoji, status_text = get_status(race_id, grid_count, driver_count)
        closed = is_registration_closed()

        weather_emoji = _weather_emoji(race.get("weather_category", ""))
        weather_text = race.get("weather_name", race.get("weather_code", ""))

        track_stats = build_track_stats_block(race)
        header = (
            f"{test_banner}"
            f"🏁 **Rennen {race['race_number']} · {race['season']}**\n"
            f"📍 {race['track_name']}\n"
            f"🔄 {race['laps']} Runden · 🕐 {race['time_of_day']} · {weather_emoji} {race['weather_code']} · {weather_text}\n"
            f"📅 {_format_date(race['race_date'])} · Lobby öffnet {LOBBY_OPEN} Uhr\n"
            + (f"\n{track_stats}\n" if track_stats else "") +
            f"\n{status_emoji} {status_text}\n"
        )

        return header, not closed

    # ── Variante B: Pause-Woche ──────────────────────────────────────────
    next_race = get_next_future_race()
    if next_race:
        reg_start = _next_tuesday()
        message = (
            f"{test_banner}"
            f"⏸️ **Nächsten Montag findet kein Rennen statt.**\n\n"
            f"🏁 Nächstes Rennen: **{_format_date(next_race['race_date'])}** auf **{next_race['track_name']}**\n"
            f"📋 Anmeldung startet am Dienstag, {reg_start} um 10:00 Uhr"
        )
        return message, False

    # ── Variante C: Kein zukünftiges Rennen ─────────────────────────────
    message = (
        f"{test_banner}"
        f"📭 **Die Saison ist beendet.**\n"
        f"Sobald der neue Rennkalender steht, wird die Anmeldung hier gestartet."
    )
    return message, False


# ─────────────────────────────────────────────
# Status-Button (ephemeral)
# ─────────────────────────────────────────────

def build_status_message(driver: dict, race_id: int, race: dict) -> str:
    """
    Baut die ephemeral Status-Nachricht für den Status-Button.
    """
    driver_id = driver["driver_id"]
    track_id = race.get("track_id")
    season_id = get_active_season_id()

    lines = []

    # ── Anmeldestatus ────────────────────────────────────────────────────
    reg = get_registration(race_id, driver_id)
    abo = has_abo(driver_id)

    if reg:
        source_text = " *(via Dauerabo)*" if reg["source"] == "abo" else ""
        lines.append(f"✅ **Du bist angemeldet**{source_text}")
    else:
        lines.append("❌ **Du bist nicht angemeldet**")

    if abo and not reg:
        lines.append("📋 Du hast eine Daueranmeldung – sie greift ab nächster Woche.")

    # ── Grid-Einteilung ──────────────────────────────────────────────────
    if reg:
        grid = get_driver_grid_assignment(driver_id, race_id)
        if grid:
            lines.append("")
            lines.append(f"📋 Du bist aktuell in **Grid {grid['grid_number']}** eingeteilt.")
            lines.append("*(Beachte: Die Einteilung kann sich bis zum Rennen noch ändern.)*")
            if grid.get("streamer_name"):
                stream_text = f"Dein Streamer ist **{grid['streamer_name']}**"
                if grid.get("streamer_url"):
                    stream_text += f" · [Stream]({grid['streamer_url']})"
                lines.append(stream_text)
            lines.append("Die komplette Grideinteilung: https://cutt.ly/RTC-infos")

    # ── Rating & Saisonstand ─────────────────────────────────────────────
    rating = get_driver_current_rating(driver_id)
    overall = get_driver_overall_stats(driver_id)
    standings = get_driver_season_standings(driver_id, season_id) if season_id else None

    lines.append("")
    info_parts = []
    if rating and rating.get("current_rating"):
        info_parts.append(f"📈 Rating: **{float(rating['current_rating']):.4f}**")
    if overall.get("total_races"):
        info_parts.append(f"🏁 Rennen gesamt: **{overall['total_races']}**")
    if standings:
        info_parts.append(f"🏆 Saison-Punkte: **{standings.get('total_points', 0)}** · Rennen: **{standings.get('races_started', 0)}**")
    for part in info_parts:
        lines.append(part)

    # ── Strecken-Ergebnisse ──────────────────────────────────────────────
    if track_id:
        stats = get_driver_track_stats(driver_id, track_id)
        lines.append("")
        lines.append(f"🏎️ **Deine bisherigen Ergebnisse** auf {race.get('track_name', '?')}:")

        if stats["race_count"] == 0:
            lines.append("Du bist diese Strecke noch nie gefahren.")
        else:
            code_lines = ["Season    Datum      Pos  Ges  Fahrzeug"]
            code_lines.append("─" * 52)
            for result in stats["top3"]:
                season = str(result.get("season_name", "?"))[:8].ljust(8)
                race_date = result.get("race_date", "")
                date_str = race_date.strftime("%d.%m.%y") if race_date else "?       "
                pos_grid = str(result.get("finish_pos_grid", "?")).rjust(3)
                pos_overall = str(result.get("finish_pos_overall", "?")).rjust(4)
                vehicle = str(result.get("vehicle_name", "?"))[:18]
                code_lines.append(f"{season}  {date_str}  {pos_grid}  {pos_overall}  {vehicle}")
            lines.append("```\n" + "\n".join(code_lines) + "\n```")

    return "\n".join(lines)
