import requests
import mysql.connector

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


def fetch_all_rows(season: int) -> list[dict]:
    """Paginate until data is empty."""
    all_rows = []
    page = 1

    while True:
        params = {
            **COMMON_PARAMS,
            "season": season,
            "statType": STAT_TYPE,
            "page": page,
        }
        r = requests.get(BASE_URL, params=params, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()

        payload = r.json()
        rows = payload.get("data", [])
        if not rows:
            break

        all_rows.extend(rows)
        page += 1

    return all_rows


def get_player_id(cur, full_name: str, position: str) -> int | None:
    cur.execute(
        "SELECT player_id FROM players WHERE full_name=%s AND position=%s",
        (full_name, position),
    )
    row = cur.fetchone()
    return row[0] if row else None


def ensure_season_row(cur, player_id: int, season: int):
    cur.execute(
        """
        INSERT INTO player_season_stats (player_id, season)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE season = season
        """,
        (player_id, season),
    )


def update_games_played(cur, player_id: int, season: int, games: int):
    cur.execute(
        """
        UPDATE player_season_stats
        SET games_played = %s
        WHERE player_id = %s AND season = %s
        """,
        (games, player_id, season),
    )


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    for season in [2021, 2022, 2023, 2024, 2025]:
        print(f"[INFO] Fetching games played for season {season}")
        rows = fetch_all_rows(season)

        updated = 0
        skipped_pos = 0
        missing = 0

        for row in rows:
            name = row.get("playerName")
            pos = row.get("position")
            stat_val = row.get("statValue")

            if not name or not pos or stat_val is None:
                continue

            if pos not in ALLOWED_POS:
                skipped_pos += 1
                continue

            pid = get_player_id(cur, name, pos)
            if not pid:
                missing += 1
                continue

            try:
                games = int(stat_val)
            except (TypeError, ValueError):
                continue

            ensure_season_row(cur, pid, season)
            update_games_played(cur, pid, season, games)
            updated += 1

        conn.commit()
        print(f"[DONE] season={season} updated={updated} skipped_non_fantasy_pos={skipped_pos} missing_players={missing}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
