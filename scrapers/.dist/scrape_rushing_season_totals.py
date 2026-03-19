import requests
import mysql.connector

import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

BASE_URL = "https://api.statrankings.com/stats-service/nfl/players/rushing"

COMMON_PARAMS = {
    "split": "ALL_GAMES",
    "rate": "SEASON",
    "sortOrder": "DESC",
    "seasonType": "REG",
    "pageSize": 100,
}

# API statType -> DB column (using your confirmed names)
RUSHING_STATS = {
    "RUSH_ATTEMPTS":        "rush_attempts",
    "RUSHING_YARDS":        "rush_yards",
    "RUSHING_TOUCHDOWNS":   "rush_tds",
}

def fetch_all_rows(season: int, stat_type: str) -> list[dict]:
    """Paginate until data is empty."""
    all_rows = []
    page = 1

    while True:
        params = {
            **COMMON_PARAMS,
            "season": season,
            "statType": stat_type,
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
    """Create (player_id, season) row if missing."""
    cur.execute(
        """
        INSERT INTO player_season_stats (player_id, season)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE season = season
        """,
        (player_id, season),
    )


def update_stat(cur, player_id: int, season: int, col: str, val: int):
    cur.execute(
        f"""
        UPDATE player_season_stats
        SET {col} = %s
        WHERE player_id = %s AND season = %s
        """,
        (val, player_id, season),
    )


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    for season in [2021, 2022, 2023, 2024, 2025]:
        for stat_type, db_col in RUSHING_STATS.items():
            print(f"[INFO] season={season} statType={stat_type} -> {db_col}")

            rows = fetch_all_rows(season, stat_type)
            updated = 0
            missing = 0

            for row in rows:
                name = row.get("playerName")
                pos = row.get("position")
                stat_val = row.get("statValue")  # confirmed by your JSON

                if not name or not pos or stat_val is None:
                    continue

                # only fantasy positions
                if pos not in ("QB", "RB", "WR", "TE"):
                    continue

                pid = get_player_id(cur, name, pos)
                if not pid:
                    missing += 1
                    continue

                try:
                    val = int(stat_val)
                except (TypeError, ValueError):
                    continue

                ensure_season_row(cur, pid, season)
                update_stat(cur, pid, season, db_col, val)
                updated += 1

            conn.commit()
            print(f"[DONE] {season} {stat_type}: updated={updated}, missing_players={missing}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
