import time
import requests
import mysql.connector
from mysql.connector import Error

import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

BASE_URL = "https://api.statrankings.com/stats-service/nfl/players/general"

COMMON_PARAMS = {
    "split": "ALL_GAMES",
    "rate": "SEASON",
    "sortOrder": "DESC",
    "seasonType": "REG",
    "pageSize": 100,
}

STAT_TYPE = "GAMES_PLAYED"
ALLOWED_POS = {"QB", "RB", "WR", "TE"}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def get_json_with_retries(url: str, params: dict, max_tries: int = 5) -> dict:
    backoff = 0.75
    for attempt in range(1, max_tries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == max_tries:
                raise
            time.sleep(backoff)
            backoff *= 1.7


def fetch_all_rows(season: int) -> list[dict]:
    """Paginate until API returns empty data list."""
    out = []
    page = 1
    while True:
        params = {**COMMON_PARAMS, "season": season, "statType": STAT_TYPE, "page": page}
        payload = get_json_with_retries(BASE_URL, params)
        rows = payload.get("data", [])
        if not rows:
            break
        out.extend(rows)
        page += 1
    return out


def get_player_row(cur, full_name: str, position: str):
    cur.execute(
        "SELECT player_id, statrankings_player_page_id FROM players WHERE full_name=%s AND position=%s",
        (full_name, position),
    )
    return cur.fetchone()  # (player_id, existing_page_id) or None


def update_page_id(cur, player_id: int, page_id: int, api_uuid: str | None):
    cur.execute(
        """
        UPDATE players
        SET statrankings_player_page_id = %s,
            statrankings_api_player_id = COALESCE(%s, statrankings_api_player_id)
        WHERE player_id = %s
        """,
        (page_id, api_uuid, player_id),
    )


def main():
    seasons = [2021, 2022, 2023, 2024, 2025]  # use whatever range you want

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    updated = 0
    already_set = 0
    missing_in_players = 0
    bad_customid = 0
    skipped_pos = 0

    for season in seasons:
        print(f"\n[INFO] Fetching {STAT_TYPE} rows for season {season}")
        rows = fetch_all_rows(season)
        print(f"[INFO] Got {len(rows)} rows")

        for row in rows:
            name = row.get("playerName")
            pos = row.get("position")
            stat_val = row.get("statValue")

            if not name or not pos or stat_val is None:
                continue
            if pos not in ALLOWED_POS:
                skipped_pos += 1
                continue

            # Only players who played > 0 games (optional but helps avoid junk)
            try:
                games = int(stat_val)
            except (TypeError, ValueError):
                continue
            if games <= 0:
                continue

            # THIS is the page id (confirmed by you)
            custom_id = row.get("customId")
            if not custom_id:
                bad_customid += 1
                continue

            try:
                page_id = int(custom_id)
            except (TypeError, ValueError):
                bad_customid += 1
                continue

            api_uuid = row.get("playerId")  # UUID string, optional storage

            db_row = get_player_row(cur, name, pos)
            if not db_row:
                missing_in_players += 1
                continue

            player_id, existing_page_id = db_row
            if existing_page_id is not None and int(existing_page_id) == page_id:
                already_set += 1
                continue

            update_page_id(cur, int(player_id), page_id, api_uuid)
            updated += 1

        conn.commit()
        print(f"[DONE] season={season} updated={updated} (running total), missing_in_players={missing_in_players}")

    cur.close()
    conn.close()

    print("\n[SUMMARY]")
    print("updated:", updated)
    print("already_set:", already_set)
    print("missing_in_players:", missing_in_players)
    print("bad_customid:", bad_customid)
    print("skipped_pos:", skipped_pos)


if __name__ == "__main__":
    try:
        main()
    except Error as db_err:
        print(f"[DB ERROR] {db_err}")
    except requests.RequestException as req_err:
        print(f"[HTTP ERROR] {req_err}")
    except Exception as e:
        print(f"[ERROR] {e}")