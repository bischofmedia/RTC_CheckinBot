"""
admin_ui.py – RTC CheckinBot Admin-UI Cog

Postet eine persistente Admin-Nachricht in CHAN_ADMIN.
Wird beim Bot-Start und jeden Dienstag um 10:00 Uhr (Berlin) aktualisiert.

Buttons (3 Rows à 2):
  Row 0: ✅ Anmelden    ❌ Abmelden      (nur wenn Rennen aktiv)
  Row 1: ⭐ Abo an      ⬜ Abo aus
  Row 2: 🔒 Sperren    🔓 Entsperren

Flow:
  Button → Fahrer-Pulldown (≤25) oder Buchstabenbereich → Fahrer-Pulldown → DB-Update

Voraussetzungen in .env:
  CHAN_ADMIN                – Channel-ID für Admin-Nachrichten
  DB_HOST, DB_USER          – MariaDB-Zugangsdaten
  DB_PASSWORD               – MariaDB-Passwort
  DB_NAME                   – Datenbankname
  GOOGLE_SHEETS_ID          – Spreadsheet-ID
  GOOGLE_CREDENTIALS_FILE   – Pfad zur Service-Account-JSON

Einbinden in checkin_bot.py:
  await bot.load_extension("admin_ui")

SQL-Voraussetzung (einmalig ausführen):
  ALTER TABLE `drivers` ADD COLUMN `abo_locked` tinyint(1) NOT NULL DEFAULT 0;
"""

import os
import logging
from datetime import date, timedelta

import discord
from discord.ext import commands, tasks
from discord import app_commands
import pymysql
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pytz

log = logging.getLogger("admin_ui")

ADMIN_MSG_MARKER = "rtc-admin-ui-v1"
BERLIN = pytz.timezone("Europe/Berlin")

# ---------------------------------------------------------------------------
# DB-Hilfsfunktionen
# ---------------------------------------------------------------------------

def get_db():
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def fetch_next_race(db) -> dict | None:
    """
    Gibt das nächste Rennen aus race_calendar zurück:
      {"race_date": date, "track_name": str, "track_id": int}
    oder None wenn kein zukünftiger Eintrag vorhanden.
    """
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT race_date, track_name, track_id
            FROM race_calendar
            WHERE race_date >= %s
            ORDER BY race_date ASC
            LIMIT 1
            """,
            (date.today(),),
        )
        return cur.fetchone()


def fetch_driver_ids_by_psn(db, psn_names: list[str]) -> dict[str, int]:
    if not psn_names:
        return {}
    placeholders = ",".join(["%s"] * len(psn_names))
    with db.cursor() as cur:
        cur.execute(
            f"SELECT driver_id, psn_name FROM drivers WHERE psn_name IN ({placeholders})",
            psn_names,
        )
        return {row["psn_name"]: row["driver_id"] for row in cur.fetchall()}


def fetch_all_status(db, psn_names: list[str]) -> dict[str, dict]:
    """
    Gibt pro PSN-Name zurück:
      driver_id, registered (bool), abo (bool), locked (bool)
    registered = Eintrag in checkin_registrations (race_id ignoriert, Tabelle
                 enthält immer nur das aktuelle Rennen).
    """
    id_map = fetch_driver_ids_by_psn(db, psn_names)
    if not id_map:
        return {}

    driver_ids   = list(id_map.values())
    placeholders = ",".join(["%s"] * len(driver_ids))

    with db.cursor() as cur:
        cur.execute(
            f"SELECT driver_id FROM checkin_registrations WHERE driver_id IN ({placeholders})",
            driver_ids,
        )
        registered_ids = {r["driver_id"] for r in cur.fetchall()}

        cur.execute(
            f"SELECT driver_id FROM checkin_abo WHERE driver_id IN ({placeholders})",
            driver_ids,
        )
        abo_ids = {r["driver_id"] for r in cur.fetchall()}

        cur.execute(
            f"SELECT driver_id, abo_locked FROM drivers WHERE driver_id IN ({placeholders})",
            driver_ids,
        )
        locked_ids = {r["driver_id"] for r in cur.fetchall() if r["abo_locked"]}

    return {
        psn: {
            "driver_id": did,
            "registered": did in registered_ids,
            "abo":        did in abo_ids,
            "locked":     did in locked_ids,
        }
        for psn, did in id_map.items()
    }


# ---------------------------------------------------------------------------
# Sheet-Hilfsfunktionen
# ---------------------------------------------------------------------------

def fetch_drivers_from_sheet() -> list[dict]:
    """
    Liest alle Fahrer aus DB_drvr (ab Zeile 5, Header Zeile 4).
    Spalte C (Index 2) = PSN-Name, Spalte J (Index 9) = Discord-Nick.
    Gibt Liste von {"psn": str, "nick": str} zurück, alphabetisch sortiert.
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        os.environ["GOOGLE_CREDENTIALS_FILE"], scope
    )
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(os.environ["GOOGLE_SHEETS_ID"]).worksheet("DB_drvr")

    all_values = sheet.get_all_values()
    drivers = []
    for row in all_values[4:]:   # Daten ab Zeile 5 (0-indexiert: Index 4+)
        psn  = row[2].strip() if len(row) > 2 else ""   # Spalte C
        nick = row[9].strip() if len(row) > 9 else ""   # Spalte J
        if psn:
            drivers.append({"psn": psn, "nick": nick})

    drivers.sort(key=lambda d: d["psn"].lstrip("|").lower())
    return drivers


# ---------------------------------------------------------------------------
# Fahrer nach Modus filtern
# ---------------------------------------------------------------------------

MODE_LABELS = {
    "anmelden":   "✅ Anmelden",
    "abmelden":   "❌ Abmelden",
    "abo_an":     "⭐ Abo an",
    "abo_aus":    "⬜ Abo aus",
    "sperren":    "🔒 Sperren",
    "entsperren": "🔓 Entsperren",
}


def _filter_drivers(mode: str, drivers: list[dict], status_map: dict) -> list[dict]:
    """Gibt nur Fahrer zurück, auf die die Aktion sinnvoll anwendbar ist."""
    def ok(d):
        st = status_map.get(d["psn"], {})
        if mode == "anmelden":   return not st.get("registered")
        if mode == "abmelden":   return     st.get("registered")
        if mode == "abo_an":     return not st.get("abo") and not st.get("locked")
        if mode == "abo_aus":    return     st.get("abo")
        if mode == "sperren":    return not st.get("locked")
        if mode == "entsperren": return     st.get("locked")
        return True
    return [d for d in drivers if ok(d)]


# ---------------------------------------------------------------------------
# Buchstabenbereich-Gruppierung (nur wenn >25 Fahrer)
# ---------------------------------------------------------------------------

def build_ranges(drivers: list[dict], max_per_group: int = 25) -> list[dict]:
    """
    Teilt Fahrerliste in Buchstabengruppen, max. 25 pro Gruppe, max. 5 Gruppen.
    """
    buckets: dict[str, list] = {}
    for d in drivers:
        letter = d["psn"].lstrip("|")[0].upper()
        buckets.setdefault(letter, []).append(d)

    letters         = sorted(buckets.keys())
    groups          = []
    current_letters = []
    current_drivers = []

    for letter in letters:
        if current_drivers and len(current_drivers) + len(buckets[letter]) > max_per_group:
            groups.append({
                "label":   f"{current_letters[0]}–{current_letters[-1]}" if len(current_letters) > 1 else current_letters[0],
                "drivers": current_drivers,
            })
            current_letters = []
            current_drivers = []
        current_letters.append(letter)
        current_drivers.extend(buckets[letter])

    if current_drivers:
        groups.append({
            "label":   f"{current_letters[0]}–{current_letters[-1]}" if len(current_letters) > 1 else current_letters[0],
            "drivers": current_drivers,
        })

    # Auf max. 5 Gruppen reduzieren
    while len(groups) > 5:
        merged = []
        for i in range(0, len(groups), 2):
            if i + 1 < len(groups):
                first = groups[i]["label"].split("–")[0]
                last  = groups[i + 1]["label"].split("–")[-1]
                merged.append({
                    "label":   f"{first}–{last}",
                    "drivers": groups[i]["drivers"] + groups[i + 1]["drivers"],
                })
            else:
                merged.append(groups[i])
        groups = merged

    return groups


# ---------------------------------------------------------------------------
# Schritt 2: Fahrer-Select
# ---------------------------------------------------------------------------

class DriverSelect(discord.ui.Select):
    def __init__(self, mode: str, drivers: list[dict]):
        self.mode = mode

        options = []
        for d in drivers:
            psn   = d["psn"]
            nick  = d["nick"]
            label = f"{psn} / {nick}" if nick and nick != psn else psn
            if len(label) > 100:
                label = label[:97] + "…"
            options.append(discord.SelectOption(label=label, value=psn))

        super().__init__(
            placeholder="Fahrer auswählen (mehrere möglich)…",
            min_values=1,
            max_values=len(options),
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        selected_psns = self.values
        db = get_db()
        try:
            id_map  = fetch_driver_ids_by_psn(db, selected_psns)
            changed = []
            errors  = []

            for psn in selected_psns:
                did = id_map.get(psn)
                if not did:
                    errors.append(f"❓ `{psn}` – nicht in DB gefunden")
                    continue
                try:
                    with db.cursor() as cur:
                        if self.mode == "anmelden":
                            cur.execute(
                                "INSERT IGNORE INTO checkin_registrations (driver_id, source) VALUES (%s,'manual')",
                                (did,),
                            )
                            changed.append(f"✅ `{psn}` angemeldet")

                        elif self.mode == "abmelden":
                            cur.execute(
                                "DELETE FROM checkin_registrations WHERE driver_id=%s",
                                (did,),
                            )
                            changed.append(f"❌ `{psn}` abgemeldet")

                        elif self.mode == "abo_an":
                            cur.execute(
                                "INSERT IGNORE INTO checkin_abo (driver_id) VALUES (%s)",
                                (did,),
                            )
                            changed.append(f"⭐ `{psn}` Abo gesetzt")

                        elif self.mode == "abo_aus":
                            cur.execute(
                                "DELETE FROM checkin_abo WHERE driver_id=%s",
                                (did,),
                            )
                            changed.append(f"⬜ `{psn}` Abo entfernt")

                        elif self.mode == "sperren":
                            cur.execute(
                                "UPDATE drivers SET abo_locked=1 WHERE driver_id=%s",
                                (did,),
                            )
                            changed.append(f"🔒 `{psn}` gesperrt")

                        elif self.mode == "entsperren":
                            cur.execute(
                                "UPDATE drivers SET abo_locked=0 WHERE driver_id=%s",
                                (did,),
                            )
                            changed.append(f"🔓 `{psn}` Sperre aufgehoben")

                except Exception as e:
                    errors.append(f"⚠️ `{psn}` – Fehler: {e}")

        finally:
            db.close()

        lines = changed + errors
        await interaction.response.send_message(
            "**Admin-Aktion abgeschlossen:**\n" + ("\n".join(lines) if lines else "Keine Änderungen."),
            ephemeral=True,
        )


class DriverSelectView(discord.ui.View):
    def __init__(self, mode: str, drivers: list[dict]):
        super().__init__(timeout=120)
        self.add_item(DriverSelect(mode, drivers))


# ---------------------------------------------------------------------------
# Schritt 1b (optional): Buchstabenbereich-Select wenn >25 Fahrer
# ---------------------------------------------------------------------------

class RangeSelect(discord.ui.Select):
    def __init__(self, mode: str, ranges: list[dict]):
        self.mode   = mode
        self.ranges = ranges

        options = [
            discord.SelectOption(label=r["label"], value=str(i))
            for i, r in enumerate(ranges)
        ]
        super().__init__(placeholder="Buchstabenbereich wählen…", options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        idx     = int(self.values[0])
        drivers = self.ranges[idx]["drivers"]

        db = get_db()
        try:
            status_map = fetch_all_status(db, [d["psn"] for d in drivers])
        finally:
            db.close()

        filtered = _filter_drivers(self.mode, drivers, status_map)
        if not filtered:
            await interaction.followup.send(
                "Keine passenden Fahrer in diesem Bereich.", ephemeral=True
            )
            return

        view = DriverSelectView(self.mode, filtered)
        await interaction.followup.send(
            f"**{MODE_LABELS[self.mode]}** – Fahrer auswählen:",
            view=view,
            ephemeral=True,
        )


class RangeSelectView(discord.ui.View):
    def __init__(self, mode: str, ranges: list[dict]):
        super().__init__(timeout=60)
        self.add_item(RangeSelect(mode, ranges))


# ---------------------------------------------------------------------------
# Admin-Views (mit/ohne Anmelden-Buttons)
# ---------------------------------------------------------------------------

class AdminViewFull(discord.ui.View):
    """Alle 6 Buttons — wenn ein Rennen ansteht."""

    def __init__(self):
        super().__init__(timeout=None)

    async def _handle(self, interaction: discord.Interaction, mode: str):
        await interaction.response.defer(ephemeral=True)

        try:
            all_drivers = fetch_drivers_from_sheet()
        except Exception as e:
            await interaction.followup.send(f"⚠️ Sheet-Fehler: {e}", ephemeral=True)
            return

        db = get_db()
        try:
            status_map = fetch_all_status(db, [d["psn"] for d in all_drivers])
        finally:
            db.close()

        filtered = _filter_drivers(mode, all_drivers, status_map)
        if not filtered:
            await interaction.followup.send(
                f"Keine Fahrer für **{MODE_LABELS[mode]}** verfügbar.", ephemeral=True
            )
            return

        if len(filtered) <= 25:
            view = DriverSelectView(mode, filtered)
            await interaction.followup.send(
                f"**{MODE_LABELS[mode]}** – Fahrer auswählen:",
                view=view, ephemeral=True,
            )
        else:
            ranges = build_ranges(filtered)
            view   = RangeSelectView(mode, ranges)
            await interaction.followup.send(
                f"**{MODE_LABELS[mode]}** – Buchstabenbereich wählen:",
                view=view, ephemeral=True,
            )

    @discord.ui.button(label="✅ Anmelden",    style=discord.ButtonStyle.success,   custom_id="adm_anmelden",   row=0)
    async def btn_anmelden(self, i, b):    await self._handle(i, "anmelden")

    @discord.ui.button(label="❌ Abmelden",    style=discord.ButtonStyle.danger,    custom_id="adm_abmelden",   row=0)
    async def btn_abmelden(self, i, b):    await self._handle(i, "abmelden")

    @discord.ui.button(label="⭐ Abo an",      style=discord.ButtonStyle.primary,   custom_id="adm_abo_an",     row=1)
    async def btn_abo_an(self, i, b):      await self._handle(i, "abo_an")

    @discord.ui.button(label="⬜ Abo aus",     style=discord.ButtonStyle.secondary, custom_id="adm_abo_aus",    row=1)
    async def btn_abo_aus(self, i, b):     await self._handle(i, "abo_aus")

    @discord.ui.button(label="🔒 Sperren",    style=discord.ButtonStyle.danger,    custom_id="adm_sperren",    row=2)
    async def btn_sperren(self, i, b):     await self._handle(i, "sperren")

    @discord.ui.button(label="🔓 Entsperren", style=discord.ButtonStyle.success,   custom_id="adm_entsperren", row=2)
    async def btn_entsperren(self, i, b):  await self._handle(i, "entsperren")


class AdminViewAboOnly(discord.ui.View):
    """Nur Abo- und Sperre-Buttons — bei Pause oder Saisonende."""

    def __init__(self):
        super().__init__(timeout=None)

    async def _handle(self, interaction: discord.Interaction, mode: str):
        await interaction.response.defer(ephemeral=True)

        try:
            all_drivers = fetch_drivers_from_sheet()
        except Exception as e:
            await interaction.followup.send(f"⚠️ Sheet-Fehler: {e}", ephemeral=True)
            return

        db = get_db()
        try:
            status_map = fetch_all_status(db, [d["psn"] for d in all_drivers])
        finally:
            db.close()

        filtered = _filter_drivers(mode, all_drivers, status_map)
        if not filtered:
            await interaction.followup.send(
                f"Keine Fahrer für **{MODE_LABELS[mode]}** verfügbar.", ephemeral=True
            )
            return

        if len(filtered) <= 25:
            view = DriverSelectView(mode, filtered)
            await interaction.followup.send(
                f"**{MODE_LABELS[mode]}** – Fahrer auswählen:",
                view=view, ephemeral=True,
            )
        else:
            ranges = build_ranges(filtered)
            view   = RangeSelectView(mode, ranges)
            await interaction.followup.send(
                f"**{MODE_LABELS[mode]}** – Buchstabenbereich wählen:",
                view=view, ephemeral=True,
            )

    @discord.ui.button(label="⭐ Abo an",      style=discord.ButtonStyle.primary,   custom_id="adm_abo_an_p",     row=0)
    async def btn_abo_an(self, i, b):      await self._handle(i, "abo_an")

    @discord.ui.button(label="⬜ Abo aus",     style=discord.ButtonStyle.secondary, custom_id="adm_abo_aus_p",    row=0)
    async def btn_abo_aus(self, i, b):     await self._handle(i, "abo_aus")

    @discord.ui.button(label="🔒 Sperren",    style=discord.ButtonStyle.danger,    custom_id="adm_sperren_p",    row=1)
    async def btn_sperren(self, i, b):     await self._handle(i, "sperren")

    @discord.ui.button(label="🔓 Entsperren", style=discord.ButtonStyle.success,   custom_id="adm_entsperren_p", row=1)
    async def btn_entsperren(self, i, b):  await self._handle(i, "entsperren")


# ---------------------------------------------------------------------------
# Embed + Nachricht bauen
# ---------------------------------------------------------------------------

def _next_monday() -> date:
    today = date.today()
    days_ahead = 7 - today.weekday()   # Montag = 0
    if days_ahead == 7:
        days_ahead = 0
    return today + timedelta(days=days_ahead)


def build_embed_and_view(next_race: dict | None) -> tuple[discord.Embed, discord.ui.View]:
    """
    Gibt (Embed, View) zurück passend zum nächsten Rennen.
    next_race: {"race_date": date, "track_name": str, "track_id": int} oder None
    """
    if next_race and next_race["track_id"] != 0:
        race_date  = next_race["race_date"]
        track_name = next_race["track_name"]
        # Ist das Rennen am nächsten Montag?
        is_monday  = race_date == _next_monday()

        if is_monday:
            desc = (
                f"**Nächstes Rennen:** {track_name} – "
                f"{race_date.strftime('%d.%m.%Y')}\n\n"
                "**✅ Anmelden / ❌ Abmelden** – Fahrer für dieses Rennen\n"
                "**⭐ Abo an / ⬜ Abo aus** – Daueranmeldung verwalten\n"
                "**🔒 Sperren / 🔓 Entsperren** – Selbst-Abo-Berechtigung"
            )
        else:
            desc = (
                f"**Nächstes eingetragenes Rennen:** {track_name} – "
                f"{race_date.strftime('%d.%m.%Y')}\n"
                f"*(kein Rennen am kommenden Montag)*\n\n"
                "**⭐ Abo an / ⬜ Abo aus** – Daueranmeldung verwalten\n"
                "**🔒 Sperren / 🔓 Entsperren** – Selbst-Abo-Berechtigung"
            )
            embed = discord.Embed(
                title="🏁 RTC CheckinBot – Admin-Verwaltung",
                description=desc,
                color=discord.Color.dark_grey(),
            )
            embed.set_footer(text=ADMIN_MSG_MARKER)
            return embed, AdminViewAboOnly()

        embed = discord.Embed(
            title="🏁 RTC CheckinBot – Admin-Verwaltung",
            description=desc,
            color=discord.Color.blue(),
        )
        embed.set_footer(text=ADMIN_MSG_MARKER)
        return embed, AdminViewFull()

    else:
        # Pause oder Saisonende
        if next_race and next_race["track_id"] == 0:
            info = "Diese Woche ist eine **Rennpause**."
        else:
            info = "Die Saison ist **beendet**."

        embed = discord.Embed(
            title="🏁 RTC CheckinBot – Admin-Verwaltung",
            description=(
                f"{info} Keine Renn-Anmeldungen möglich.\n\n"
                "**⭐ Abo an / ⬜ Abo aus** – Daueranmeldung verwalten\n"
                "**🔒 Sperren / 🔓 Entsperren** – Selbst-Abo-Berechtigung"
            ),
            color=discord.Color.dark_grey(),
        )
        embed.set_footer(text=ADMIN_MSG_MARKER)
        return embed, AdminViewAboOnly()


# ---------------------------------------------------------------------------
# Nachricht finden / aktualisieren
# ---------------------------------------------------------------------------

async def find_admin_message(channel: discord.TextChannel) -> discord.Message | None:
    async for msg in channel.history(limit=50):
        if msg.author == channel.guild.me and msg.embeds:
            if (msg.embeds[0].footer.text or "") == ADMIN_MSG_MARKER:
                return msg
    return None


async def update_admin_message(bot: commands.Bot, force_repost: bool = False) -> None:
    """
    Aktualisiert die Admin-Nachricht in CHAN_ADMIN.
    Existiert sie noch nicht, wird sie gepostet.
    Mit force_repost=True wird sie gelöscht und neu gepostet.
    """
    chan_id = int(os.environ["CHAN_ADMIN"])
    channel = bot.get_channel(chan_id)
    if not channel:
        log.error("CHAN_ADMIN %s nicht gefunden.", chan_id)
        return

    db = get_db()
    try:
        next_race = fetch_next_race(db)
    finally:
        db.close()

    embed, view = build_embed_and_view(next_race)
    existing    = await find_admin_message(channel)

    if existing and not force_repost:
        await existing.edit(embed=embed, view=view)
        log.info("Admin-Nachricht aktualisiert (ID %s).", existing.id)
    else:
        if existing:
            await existing.delete()
        await channel.send(embed=embed, view=view)
        log.info("Admin-Nachricht in CHAN_ADMIN %s gepostet.", chan_id)


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------

class AdminUI(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # Persistente Views registrieren damit Buttons nach Neustart funktionieren
        bot.add_view(AdminViewFull())
        bot.add_view(AdminViewAboOnly())
        self.tuesday_update.start()

    async def cog_load(self):
        await update_admin_message(self.bot)

    def cog_unload(self):
        self.tuesday_update.cancel()

    @tasks.loop(minutes=1)
    async def tuesday_update(self):
        """Aktualisiert die Admin-Nachricht jeden Dienstag um 10:00 Uhr Berlin."""
        now = discord.utils.utcnow().astimezone(BERLIN)
        if now.weekday() == 1 and now.hour == 10 and now.minute == 0:
            log.info("Dienstags-Update: Admin-Nachricht wird aktualisiert.")
            await update_admin_message(self.bot)

    @tuesday_update.before_loop
    async def before_tuesday_update(self):
        await self.bot.wait_until_ready()

    @app_commands.command(name="admin-post", description="Admin-Nachricht löschen und neu posten (Backup).")
    @app_commands.default_permissions(administrator=True)
    async def admin_post(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await update_admin_message(self.bot, force_repost=True)
        chan_id = int(os.environ["CHAN_ADMIN"])
        await interaction.followup.send(
            f"✅ Admin-Nachricht in <#{chan_id}> neu gepostet.", ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminUI(bot))
