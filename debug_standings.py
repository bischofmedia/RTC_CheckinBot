#!/usr/bin/env python3
"""
Debug-Script: Saisonstand mit Streichern für einen Fahrer ausgeben.
Aufruf: python3 debug_standings.py <psn_name>
"""
import sys
import os
from dotenv import load_dotenv

load_dotenv("/home/ubuntu/RTC_CheckinBot/.env")

import pymysql
from datetime import date

def get_conn():
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

psn = sys.argv[1] if len(sys.argv) > 1 else input("PSN-Name: ")

conn = get_conn()
with conn.cursor() as cur:
    # Driver-ID
    cur.execute("SELECT driver_id FROM drivers WHERE psn_name = %s", (psn,))
    row = cur.fetchone()
    if not row:
        print(f"Fahrer '{psn}' nicht gefunden.")
        sys.exit(1)
    driver_id = row["driver_id"]

    # Aktive Saison
    cur.execute("SELECT * FROM seasons WHERE is_active = 1 LIMIT 1")
    season = cur.fetchone()
    season_id = season["season_id"]
    print(f"\nSaison: {season['name']} (ID {season_id})")
    print(f"Streicher-Schwellen: nach R{season['drop_after_race_1']} / R{season['drop_after_race_2']} / R{season['drop_after_race_3']}")
    print(f"Max. Streicher: {season['drop_results']}")

    # Stattgefundene Rennen
    cur.execute("""
        SELECT COUNT(*) AS cnt, MAX(race_number) AS max_rn
        FROM races WHERE season_id = %s AND race_date <= %s
    """, (season_id, date.today()))
    sr = cur.fetchone()
    races_held = sr["cnt"]
    max_rn = sr["max_rn"]
    print(f"\nStattgefundene Rennen: {races_held} (höchste race_number: {max_rn})")

    # Ergebnisse des Fahrers
    cur.execute("""
        SELECT r.race_number, r.race_date, rr.points_total, rr.points_base, rr.bonus_total, rr.status
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WHERE rr.driver_id = %s AND r.season_id = %s
        ORDER BY r.race_number ASC
    """, (driver_id, season_id))
    results = cur.fetchall()

    print(f"\nTeilnahmen: {len(results)} von {races_held}")
    print(f"DNS (nicht in DB): {races_held - len(results)}")
    print(f"\n{'R':>3}  {'Datum':<12} {'Basis':>6} {'Bonus':>6} {'Total':>6}  Status")
    print("─" * 50)
    for r in results:
        print(f"R{r['race_number']:>2}  {str(r['race_date']):<12} {r['points_base']:>6} {r['bonus_total']:>6} {r['points_total']:>6}  {r['status'] or 'OK'}")

    # Streichungsberechnung
    dns_count = max(0, races_held - len(results))
    all_pts = sorted([r["points_total"] or 0 for r in results] + [0] * dns_count)
    gross = sum(r["points_total"] or 0 for r in results)

    active_drops = 0
    if season["drop_after_race_1"] and max_rn >= season["drop_after_race_1"]:
        active_drops = 1
    if season["drop_after_race_2"] and max_rn >= season["drop_after_race_2"]:
        active_drops = 2
    if season["drop_after_race_3"] and max_rn >= season["drop_after_race_3"]:
        active_drops = 3
    active_drops = min(active_drops, season["drop_results"] or 0)

    dropped_vals = all_pts[:active_drops]
    points_dropped = sum(v for v in dropped_vals if v > 0)
    net = gross - points_dropped

    print(f"\nAlle Punkte (aufsteigend, inkl. DNS-Nullen): {all_pts}")
    print(f"Aktive Streicher: {active_drops}")
    print(f"Gestrichene Werte: {dropped_vals}")
    print(f"Brutto: {gross}  |  -Streicher: {points_dropped}  |  Netto: {net}")

    # Gesamttabelle (Top 20 + eigene Position)
    cur.execute("""
        SELECT d.driver_id, d.psn_name
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        JOIN drivers d ON d.driver_id = rr.driver_id
        WHERE r.season_id = %s
        GROUP BY d.driver_id, d.psn_name
    """, (season_id,))
    all_drivers = cur.fetchall()

    standings = []
    for d in all_drivers:
        cur.execute("""
            SELECT rr.points_total, r.race_number
            FROM race_results rr
            JOIN races r ON r.race_id = rr.race_id
            WHERE rr.driver_id = %s AND r.season_id = %s
        """, (d["driver_id"], season_id))
        d_res = cur.fetchall()
        d_dns = max(0, races_held - len(d_res))
        d_all = sorted([r["points_total"] or 0 for r in d_res] + [0] * d_dns)
        d_gross = sum(r["points_total"] or 0 for r in d_res)
        d_drops = min(active_drops, season["drop_results"] or 0)
        d_dropped = sum(v for v in d_all[:d_drops] if v > 0)
        d_net = d_gross - d_dropped
        standings.append((d["psn_name"], d_net, d_gross, len(d_res)))

    standings.sort(key=lambda x: x[1], reverse=True)
    print(f"\n{'Pos':>4}  {'PSN':<25} {'Netto':>6} {'Brutto':>7} {'Rennen':>7}")
    print("─" * 55)
    for i, (name, n, g, rc) in enumerate(standings[:25], 1):
        marker = " ◄" if name == psn else ""
        print(f"{i:>4}  {name:<25} {n:>6} {g:>7} {rc:>7}{marker}")

    my_pos = next((i+1 for i, (name,_,_,_) in enumerate(standings) if name == psn), None)
    if my_pos and my_pos > 25:
        print(f"  ...")
        name, n, g, rc = standings[my_pos-1]
        print(f"{my_pos:>4}  {name:<25} {n:>6} {g:>7} {rc:>7} ◄")

conn.close()
