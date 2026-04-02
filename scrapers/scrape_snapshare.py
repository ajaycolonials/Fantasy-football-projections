"""
Scrape snap share JSON and use it ONLY to populate/update the `players` table.

Usage:
    cd fantasy_app/scrapers
    python scrape_snapshare.py
"""

import requests
import mysql.connector
from mysql.connector import Error

# ----------------------------
# 1. MySQL connection settings
# ----------------------------
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

# ---------------------------------------
# 2. API URL template from DevTools
# ---------------------------------------
# In DevTools Network, right-click the JSON "usage" request → Copy link address.
# It should look something like:
#   https://.../nfl/advanced-stats/usage?season=2025&position=QB&statType=snapRate
#
# Replace the season and position values with {season} and {position}:

API_URL_TEMPLATE = (
    "https://api.statrankings.com/stats-service/nfl/players/advanced/usage"
    "?season={season}&statType=SNAP_SHARE&sortOrder=DESC&page={page}&pageSize=100&position={position}"
)


# ---------------------------------------
# 3. Team name → abbreviation map
# ---------------------------------------
TEAM_MAP = {
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV",
    "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NE",
    "New Orleans Saints": "NO",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN",
    "Washington Commanders": "WAS",
}


# ----------------------------
# DB helpers
# ----------------------------

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def get_team_id(cur, team_name: str | None):
    """Map 'Tennessee Titans' -> 'TEN' -> teams.team_id."""
    if not team_name:
        return None

    abbrev = TEAM_MAP.get(team_name)
    if not abbrev:
        print(f"[WARN] Unknown team name '{team_name}' – add it to TEAM_MAP.")
        return None

    cur.execute("SELECT team_id FROM teams WHERE abbreviation = %s", (abbrev,))
    row = cur.fetchone()
    if not row:
        print(f"[WARN] No team_id found for abbreviation '{abbrev}' in teams table.")
        return None

    return row[0]


def upsert_player(cur, full_name: str, position: str, team_name: str | None):
    """
    Insert or update a player in the `players` table.

    - Look up by (full_name, position).
    - If exists: update team_id.
    - If not exists: insert new row with status='active'.
    """
    team_id = get_team_id(cur, team_name)

    cur.execute(
        "SELECT player_id FROM players WHERE full_name = %s AND position = %s",
        (full_name, position),
    )
    row = cur.fetchone()

    if row:
        player_id = row[0]
        cur.execute(
            "UPDATE players SET team_id = %s WHERE player_id = %s",
            (team_id, player_id),
        )
        print(f"[UPDATE] {full_name} ({position}) → team_id={team_id}")
    else:
        cur.execute(
            """
            INSERT INTO players (full_name, position, team_id, status)
            VALUES (%s, %s, %s, 'active')
            """,
            (full_name, position, team_id),
        )
        print(f"[INSERT] {full_name} ({position}) → team_id={team_id}")


# ----------------------------
# Scraping logic
# ----------------------------

def fetch_snapshare_json(season: int, position: str) -> list[dict]:
    """
    Fetch ALL pages for snapshare / usage API.
    """
    all_rows = []
    page = 1

    while True:
        url = API_URL_TEMPLATE.format(
            season=season,
            position=position,
            page=page
        )

        print(f"[INFO] Fetching page {page} for {position} {season}")

        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()

        payload = resp.json()
        rows = payload.get("data", [])

        if not rows:
            print(f"[INFO] No more rows at page {page}. Done.")
            break

        all_rows.extend(rows)
        page += 1

    print(f"[INFO] Total rows fetched: {len(all_rows)}")
    return all_rows

def scrape_snapshare_for_position(season: int, position: str):
    """
    Fetch snapshare data for a given season + position
    and upsert players into the `players` table only.
    """
    rows = fetch_snapshare_json(season, position)

    conn = get_connection()
    cur = conn.cursor()

    for r in rows:
        # Using your JSON format exactly:
        # {
        #   "rank": 1,
        #   "playerId": "...",
        #   "playerName": "Cam Ward",
        #   "playerShortName": "C. Ward",
        #   "position": "QB",
        #   "teamId": "...",
        #   "teamName": "Tennessee Titans",
        #   ...
        # }

        full_name = r.get("playerName")
        pos       = r.get("position")
        team_name = r.get("teamName")

        if not full_name or not pos:
            continue

        # Only keep fantasy positions
        if pos not in ("QB", "RB", "WR", "TE"):
            continue

        upsert_player(cur, full_name, pos, team_name)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[DONE] Finished players for {position} {season}")


def main():
    season = 2023  # change this if you want another season

    for pos in ["QB", "RB", "WR", "TE"]:
        scrape_snapshare_for_position(season, pos)


if __name__ == "__main__":
    try:
        main()
    except Error as db_err:
        print(f"[DB ERROR] {db_err}")
    except requests.RequestException as req_err:
        print(f"[HTTP ERROR] {req_err}")
    except Exception as e:
        print(f"[ERROR] {e}")
