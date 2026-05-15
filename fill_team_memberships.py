"""
fill_team_memberships.py
Traegt fuer jeden Fahrer seine letzte Team-Zuordnung in team_memberships ein,
basierend auf den race_results (letztes Rennen in dem er gestartet ist).
"""
import os
from dotenv import load_dotenv
load_dotenv()
import pymysql
import pymysql.cursors

conn = pymysql.connect(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", 3306)),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    cursorclass=pymysql.cursors.DictCursor,
    charset="utf8mb4",
)

with conn:
    with conn.cursor() as cur:
        # Letztes Team pro Fahrer aus race_results holen
        cur.execute("""
            SELECT rr.driver_id, rr.team_id, rr.race_id
            FROM race_results rr
            INNER JOIN (
                SELECT driver_id, MAX(race_id) AS max_race_id
                FROM race_results
                WHERE team_id IS NOT NULL
                GROUP BY driver_id
            ) latest ON rr.driver_id = latest.driver_id
                     AND rr.race_id  = latest.max_race_id
            WHERE rr.team_id IS NOT NULL
        """)
        rows = cur.fetchall()
        print(f"{len(rows)} Fahrer mit Team-Zuordnung gefunden.")

        inserted = 0
        skipped  = 0
        for row in rows:
            cur.execute(
                "INSERT IGNORE INTO team_memberships (driver_id, team_id, race_id) "
                "VALUES (%s, %s, %s)",
                (row["driver_id"], row["team_id"], row["race_id"])
            )
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        print(f"Eingetragen: {inserted}, bereits vorhanden: {skipped}")
