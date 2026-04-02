# import_redzone_rushing_core.py

import time
import requests
import mysql.connector

# ----------------------------
# Config
# ----------------------------
SEASONS = [2021, 2022, 2023, 2024, 2025]
POSITIONS = ["QB", "RB"]
INSIDE_VALUES = [20, 5]

BASE_URL = "https://api.statrankings.com/stats-service/nfl/players/advanced/red-zone/rushing"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Box60Boat",
    "database": "fantasy_app",
}

HEADERS = {
    "accept": "application/json",
}

PAGE_SIZE = 100


# ----------------------------
# DB helpers
# ----------------------------
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def ensure_season_row(cur, player_id, season):
    cur.execute("""
        INSERT INTO player_season_stats (player_id, season)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE season = season
    """, (player_id, season))


def update_stats(cur, player_id, season, inside, attempts, share, conversion):
    # Convert percentages → decimals
    share = float(share) / 100 if share is not None else None
    conversion = float(conversion) / 100 if conversion is not None else None

    if inside == 20:
        cur.execute("""
            UPDATE player_season_stats
            SET
                rz_rush_attempts = %s,
                rz_rush_share = %s
            WHERE player_id = %s AND season = %s
        """, (attempts, share, player_id, season))

    elif inside == 5:
        cur.execute("""
            UPDATE player_season_stats
            SET
                carries_inside_5 = %s,
                inside5_share = %s,
                td_rate_inside_5 = %s
            WHERE player_id = %s AND season = %s
        """, (attempts, share, conversion, player_id, season))


# ----------------------------
# API fetch
# ----------------------------
def fetch_page(season, position, inside, page):
    params = {
        "season": season,
        "inside": inside,
        "sortOrder": "DESC",
        "seasonType": "REG",
        "page": page,
        "pageSize": PAGE_SIZE,
        "position": position,
        "sortBy": "rushingAttempts",
    }

    r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ----------------------------
# Main
# ----------------------------
def run():
    conn = get_conn()
    cur = conn.cursor()

    try:
        for season in SEASONS:
            for position in POSITIONS:
                for inside in INSIDE_VALUES:

                    print(f"\n[INFO] {season} {position} inside={inside}")

                    page = 1

                    while True:
                        data = fetch_page(season, position, inside, page).get("data", [])

                        if not data:
                            break

                        for row in data:
                            player_id = row.get("playerId")
                            if not player_id:
                                continue

                            attempts = row.get("rushingAttempts")
                            share = row.get("shareOfTeamRzRushAttempts")
                            conversion = row.get("conversionRate")

                            ensure_season_row(cur, player_id, season)

                            update_stats(
                                cur,
                                player_id,
                                season,
                                inside,
                                attempts,
                                share,
                                conversion
                            )

                        conn.commit()
                        page += 1
                        time.sleep(0.2)

        print("\n[DONE] Red zone rushing stats updated.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()