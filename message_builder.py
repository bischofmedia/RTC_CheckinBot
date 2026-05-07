# v2
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
    grids = max(1, (driver_count + DRIVERS_PER_GRID - 1) // DRIVERS_PER_GRID)
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


def get_status(race_id: int, grid_count: int, driver_count: int, grid_locked: bool = False) -> tuple[str, str]:
    """
    Gibt (emoji, text) für den aktuellen Anmeldestatus zurück.
    Berücksichtigt: offen, Warteliste, geschlossen (🔴), grid_locked (🔒)
    """
    now = datetime.now(BERLIN)
    deadline_str = os.environ.get("REGISTRATION_DEADLINE", "20:45")
    h, m = map(int, deadline_str.split(":"))

    # Montag nach Deadline → 🔴
    if now.weekday() == 0:  # Montag
        deadline = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if now >= deadline:
            return "🔴", "Anmeldung geschlossen"

    # Montag nach Anmeldeschluss → 🔴
    if now.weekday() == 0:
        deadline_str = os.environ.get("REGISTRATION_DEADLINE", "20:45")
        h, m = map(int, deadline_str.split(":"))
        if now.hour > h or (now.hour == h and now.minute >= m):
            return "🔴", f"Anmeldung geschlossen · {driver_count} Fahrer · {grid_count} Grids"

    # Dienstag vor 10:00 (vor Reset) → 🔴
    if now.weekday() == 1 and now.hour < 10:
        return "🔴", f"Anmeldung geschlossen · {driver_count} Fahrer · {grid_count} Grids"

    # Vor Sonntag 18:00: max = MAX_GRIDS × DRIVERS_PER_GRID
    # Ab Sonntag 18:00: max = aktuelle Grids × DRIVERS_PER_GRID (fixiert)
    sunday_locked = (now.weekday() == 6 and now.hour >= 18)
    if sunday_locked:
        max_drivers = grid_count * DRIVERS_PER_GRID
    else:
        max_drivers = MAX_GRIDS * DRIVERS_PER_GRID
    free_slots = max_drivers - driver_count

    lock_suffix = " 🔒" if grid_locked else ""
    if free_slots <= 0:
        return "🟡", f"Warteliste aktiv · {driver_count} Fahrer · {grid_count} Grids{lock_suffix}"

    # Normal → 🟢
    return "🟢", f"Anmeldung offen · {driver_count} Fahrer · {grid_count} Grids{lock_suffix}"


def _is_grid_locked(race_id: int) -> bool:
    """Gibt True zurück wenn der Grid-Lock aktiv ist (Sonntag 18:00 oder manuell)."""
    from db import get_grid_override
    now = datetime.now(BERLIN)
    # Sunday 18:00+
    if now.weekday() == 6 and now.hour >= 18:
        return True
    # Monday
    if now.weekday() == 0:
        return True
    # Manual override
    if race_id and get_grid_override(race_id):
        return True
    return False


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

def _ts_str(ts) -> str:
    """Formatiert einen Timestamp als 'WD HH:MM'."""
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    weekday = WEEKDAYS_DE.get(ts.weekday(), "??")
    return f"{weekday} {ts.strftime('%H:%M')}"


def _get_driver_color(prev_status: str | None) -> str:
    """Gibt die Emoji-Farbe basierend auf dem vorherigen Status zurück."""
    if prev_status == "abgemeldet" or prev_status is None:
        return "🔴"
    if prev_status in ("angemeldet", "abo_angemeldet"):
        return "🟢"
    return "🟢"


def _format_log_entry(entry: dict, prev_status: str | None, is_waitlist: bool = False) -> str | None:
    """
    Formatiert einen Log-Eintrag als Discord-Zeile.
    is_waitlist: ob der Fahrer zum Zeitpunkt der Anmeldung auf der Warteliste landet.
    """
    ts = _ts_str(entry["registered_at"])
    name = entry.get("psn_name") or entry.get("discord_name") or "Unbekannt"
    action = entry["action"]

    if action == "angemeldet":
        if prev_status == "abgemeldet":
            if is_waitlist:
                return f"{ts} 🔴 -> 🟡 {name}"
            return f"{ts} 🔴 -> 🟢 {name}"
        if is_waitlist:
            return f"{ts} 🟢 -> 🟡 {name}"
        return f"{ts} 🟢 {name}"
    elif action == "abo_angemeldet":
        return f"{ts} 🟢 {name} (Abo)"
    elif action == "abgemeldet":
        if prev_status in ("angemeldet", "abo_angemeldet"):
            return f"{ts} 🟢 -> 🔴 {name}"
        if prev_status == "warteliste":
            return f"{ts} 🟡 -> 🔴 {name}"
        return f"{ts} 🔴 {name}"
    return None


def build_log_section(race_id: int) -> str:
    """
    Baut den Log-Bereich sequenziell.
    Trackt Warteliste genau: wer ist wann auf die Warteliste gekommen,
    wer rückt nach wenn ein Grid-Fahrer abmeldet.
    """
    entries = get_log_entries(race_id)
    if not entries:
        return ""

    import os as _os
    _dpg = int(_os.environ.get("DRIVERS_PER_GRID", 15))
    _mg = int(_os.environ.get("MAX_GRIDS", 4))

    # Kapazität dynamisch: Grid-Override und grid_locked berücksichtigen
    try:
        import sys as _sys
        _cb = _sys.modules.get("__main__") or _sys.modules.get("checkin_bot")
        _cb_state = getattr(_cb, "state", {}) if _cb else {}
        _locked = _cb_state.get("grid_locked", False)
        if _locked:
            _max_capacity = _cb_state.get("last_grid_count", _mg) * _dpg
        else:
            _max_capacity = _mg * _dpg
    except Exception:
        _max_capacity = _mg * _dpg

    driver_status = {}       # driver_id -> aktueller Status: 'grid', 'warteliste', 'abgemeldet'
    grid_drivers = []        # Reihenfolge der Grid-Fahrer
    waitlist_drivers = []    # Reihenfolge der Wartelisten-Fahrer (FIFO)
    lines = []

    for entry in entries:
        name = entry.get("psn_name") or entry.get("discord_name") or "Unbekannt"
        action = entry["action"]
        driver_id = entry["driver_id"]
        ts = _ts_str(entry["registered_at"])
        prev = driver_status.get(driver_id)

        if action in ("angemeldet", "abo_angemeldet"):
            # Bereits angemeldet? Ignorieren
            if prev in ("grid", "warteliste"):
                continue
            grid_count = len(grid_drivers)
            if grid_count < _max_capacity:
                # Platz im Grid
                driver_status[driver_id] = "grid"
                grid_drivers.append(driver_id)
                suffix = " (Abo)" if action == "abo_angemeldet" else ""
                if prev == "abgemeldet":
                    lines.append(f"{ts} 🔴 -> 🟢 {name}{suffix}")
                else:
                    lines.append(f"{ts} 🟢 {name}{suffix}")
            else:
                # Warteliste
                driver_status[driver_id] = "warteliste"
                waitlist_drivers.append(driver_id)
                if prev == "abgemeldet":
                    lines.append(f"{ts} 🔴 -> 🟡 {name}")
                else:
                    lines.append(f"{ts} 🟢 -> 🟡 {name}")

        elif action == "abgemeldet":
            if prev == "grid":
                driver_status[driver_id] = "abgemeldet"
                if driver_id in grid_drivers:
                    grid_drivers.remove(driver_id)
                lines.append(f"{ts} 🟢 -> 🔴 {name}")
                # Nachrücker von Warteliste
                if waitlist_drivers:
                    next_id = waitlist_drivers.pop(0)
                    next_name = next(
                        (e.get("psn_name") or e.get("discord_name") for e in entries if e["driver_id"] == next_id),
                        "Unbekannt"
                    )
                    driver_status[next_id] = "grid"
                    grid_drivers.append(next_id)
                    lines.append(f"{ts} 🟡 -> 🟢 {next_name}")
            elif prev == "warteliste":
                driver_status[driver_id] = "abgemeldet"
                if driver_id in waitlist_drivers:
                    waitlist_drivers.remove(driver_id)
                lines.append(f"{ts} 🟡 -> 🔴 {name}")
            else:
                # War nicht angemeldet, ignorieren
                continue

    if not lines:
        return ""
    return "```\n" + "\n".join(lines) + "\n```"



def build_track_stats_block(race: dict) -> str:
    """Baut den Streckenstatistik-Block für den Channel-Header."""
    track_id = race.get("track_id")
    if not track_id:
        return ""
    try:
        from db import get_track_header_stats, get_active_season_id
        # Aktuelle Saison-Klasse ermitteln
        season_class = None
        try:
            from db import get_connection
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT `class` AS season_class FROM seasons WHERE is_active = 1 LIMIT 1")
                    row = cur.fetchone()
                    if row:
                        season_class = row.get("season_class")
        except Exception:
            pass
        stats = get_track_header_stats(track_id, season_class)
    except Exception:
        return ""

    if stats["total_races"] == 0:
        return "📊 RTC fährt diese Strecke zum ersten Mal!"

    lines = []
    last = f" · zuletzt in {stats['last_season']}" if stats.get("last_season") else ""
    lines.append(f"📊 RTC war hier {stats['total_races']}×{last}")

    if stats["top_winners"]:
        winners = ", ".join(f"{w['psn_name']} ({w['wins']}×)" for w in stats["top_winners"])
        lines.append(f"🏆 Rekordsieger: {winners}")

    if stats["top_vehicles"]:
        cars = ", ".join(f"{v['vehicle_name']} ({v['cnt']}×)" for v in stats["top_vehicles"])
        lines.append(f"🚗 Top used cars: {cars}")

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
        import sys as _sys
        _cb = _sys.modules.get("__main__") or _sys.modules.get("checkin_bot")
        _grid_locked = getattr(_cb, "state", {}).get("grid_locked", False) if _cb else False
        status_emoji, status_text = get_status(race_id, grid_count, driver_count, grid_locked=_grid_locked)
        closed = is_registration_closed()

        weather_emoji = _weather_emoji(race.get("weather_category", ""))
        locked = is_registration_closed() or _is_grid_locked(race_id)
        lock_symbol = " 🔒" if locked else ""

        track_stats = build_track_stats_block(race)

        # Apollo-style: Status + Fahrer/Grids oben, dann Renninfo, dann Streckeninfo
        header = (
            f"{test_banner}"
            f"**RTC {race.get('season', '?')} · Race {race.get('race_number', '?')} · {race.get('track_name', '?')}**\n"
            f"🔄 {race.get('laps', '?')} Runden · 🕐 {race.get('time_of_day', '?')} · {weather_emoji} {race.get('weather_code', '?')}\n"
            f"📅 {_format_date(race.get('race_date'))} · Lobby öffnet {LOBBY_OPEN} Uhr"
            + (f"\n\n{track_stats}" if track_stats else "") +
            f"\n\n{status_emoji} {status_text}{lock_symbol}"
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
    try:
        driver_id = driver["driver_id"]
        track_id = race.get("track_id")
        season_id = get_active_season_id()

        lines = []

        # ── Anmeldestatus ─────────────────────────────────────────────────
        try:
            reg = get_registration(race_id, driver_id)
            abo = has_abo(driver_id)
            if reg:
                source_text = " *(via Dauerabo)*" if reg["source"] == "abo" else ""
                lines.append(f"✅ **Du bist angemeldet**{source_text}")
            else:
                lines.append("❌ **Du bist nicht angemeldet**")
            if abo and not reg:
                lines.append("📋 Du hast eine Daueranmeldung – sie greift ab nächster Woche.")
        except Exception:
            lines.append("⚠️ Anmeldestatus konnte nicht geladen werden.")

        # ── Wartelisten-Status prüfen ────────────────────────────────────
        on_waitlist = False
        try:
            if reg:
                from db import get_all_registrations
                from datetime import datetime
                _now = datetime.now(BERLIN)
                _sunday_locked = (_now.weekday() == 6 and _now.hour >= 18) or _now.weekday() == 0
                _grid_count = get_current_grid_count(race_id, get_registration_count(race_id))
                _capacity = _grid_count * DRIVERS_PER_GRID if _sunday_locked else MAX_GRIDS * DRIVERS_PER_GRID
                _all_regs = get_all_registrations(race_id)
                _pos = next((i for i, r in enumerate(_all_regs) if r["driver_id"] == driver_id), None)
                on_waitlist = _pos is not None and _pos >= _capacity
                if on_waitlist:
                    lines.append(f"🟡 **Du stehst auf der Warteliste** (Position {_pos - _capacity + 1}).")
        except Exception:
            pass

        # ── Grid-Einteilung ───────────────────────────────────────────────
        try:
            if reg and not on_waitlist:
                grid = get_driver_grid_assignment(driver_id, race_id)
                if grid:
                    lines.append("")
                    is_host = bool(grid.get("host_name") and grid["host_name"] == driver.get("psn_name"))
                    if is_host:
                        lines.append(f"📋 Du hostest **Grid {grid['grid_number']}**.")
                    else:
                        host_text = f", Dein Host ist **{grid['host_name']}**" if grid.get("host_name") else ""
                        lines.append(f"📋 Du bist aktuell in **Grid {grid['grid_number']}** eingeteilt{host_text}.")
                    if not is_host:
                        lines.append("*(Beachte: Die Einteilung kann sich bis zum Rennen noch ändern.)*")
                    if grid.get("streamer_name"):
                        stream_text = f"🎥 Dein Streamer ist **{grid['streamer_name']}**"
                        if grid.get("streamer_url"):
                            stream_text += f" · <{grid['streamer_url']}>"
                        lines.append(stream_text)
                    lines.append("📊 Die komplette Grideinteilung: <https://cutt.ly/RTC-infos>")
        except Exception:
            pass

        # ── Rating & Saisonstand ──────────────────────────────────────────
        try:
            rating = get_driver_current_rating(driver_id)
            overall = get_driver_overall_stats(driver_id)
            from db import get_driver_season_standings_with_drops
            standings = get_driver_season_standings_with_drops(driver_id, season_id) if season_id else None

            lines.append("")
            info_parts = []
            if rating and rating.get("current_rating"):
                info_parts.append(f"📈 Rating: **{float(rating['current_rating']):.4f}**")
            if overall.get("total_races"):
                info_parts.append(f"🏁 Rennen gesamt: **{overall['total_races']}**")
            if standings:
                net = standings["points_total_net"]
                gross = standings["points_total_gross"]
                drops = standings["active_drops"]
                dropped_pts = standings["points_dropped"]
                pos = standings["position"]
                total = standings["total_drivers"]
                races = standings["races_started"]
                pos_text = f"P{pos}" if pos else "?"
                info_parts.append(f"🏆 Saison: {pos_text} · **{net}** Punkte · {races} Rennen")

            for part in info_parts:
                lines.append(part)
        except Exception:
            pass

        # ── Strecken-Ergebnisse ───────────────────────────────────────────
        try:
            if track_id:
                stats = get_driver_track_stats(driver_id, track_id)
                lines.append("")
                if not reg:
                    # Nicht angemeldet
                    if stats["race_count"] == 0:
                        lines.append("🏁 Du bist noch nie auf dieser Strecke gefahren – meld dich an und setz ein Ausrufezeichen!")
                    else:
                        lines.append(f"🏎️ **Deine bisherigen Ergebnisse** auf {race.get('track_name', '?')}:")
                        lines.append("*Handy quer, siehste mehr* 😉")
                else:
                    if stats["race_count"] == 0:
                        lines.append("🏎️ Diese Strecke fährst du zum ersten Mal – viel Erfolg!")
                    else:
                        lines.append(f"🏎️ **Deine bisherigen Ergebnisse** auf {race.get('track_name', '?')}:")
                        lines.append("*Handy quer, siehste mehr* 😉")
                if not reg and stats["race_count"] == 0:
                    lines.append("👉 Meld dich jetzt an!")
                if stats["race_count"] > 0:
                    code_lines = ["Saison   Datum    Gr P  G      %  Auto"]
                    code_lines.append("─" * 42)
                    for result in stats["top3"]:
                        try:
                            season = str(result.get("season_name", "?"))[:7].ljust(7)
                            race_date = result.get("race_date", "")
                            date_str = race_date.strftime("%d.%m.%y") if race_date else "?      "
                            grid_id = str(result.get("grid_number", "?")).rjust(2)
                            pos_grid = str(result.get("finish_pos_grid", "?")).rjust(2)
                            pos_overall = str(result.get("finish_pos_overall", "?")).rjust(2)
                            pct = result.get("time_percent")
                            pct_str = f"{float(pct):.2f}%" if pct else "?"
                            pct_str = pct_str.rjust(7)
                            vehicle = str(result.get("vehicle_name", "?"))[:13]
                            code_lines.append(f"{season} {date_str} {grid_id} {pos_grid} {pos_overall} {pct_str}  {vehicle}")
                        except Exception:
                            continue
                    lines.append("```\n" + "\n".join(code_lines) + "\n```")
                if not reg and stats["race_count"] > 0:
                    lines.append("👉 Du kennst die Strecke – meld dich an!")
        except Exception:
            pass

        return "\n".join(lines)

    except Exception as e:
        return f"⚠️ Status konnte nicht geladen werden: {e}"
