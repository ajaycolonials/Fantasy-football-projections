import requests
import mysql.connector
from typing import Optional
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

BASE = "https://api.statrankings.com/stats-service/nfl/players"

SEASONS = [2021, 2022, 2023, 2024, 2025]


def parse_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_int(x) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def parse_pct_to_decimal(x) -> Optional[float]:
    val = parse_float(x)
    if val is None:
        return None
    return val / 100.0


def fetch_all_rows(full_url: str, params: dict) -> list[dict]:
    all_rows = []
    page = 1

    while True:
        p = dict(params)
        p["page"] = page

        r = requests.get(full_url, params=p, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("data", [])

        if not rows:
            break

        all_rows.extend(rows)
        page += 1

    return all_rows


def get_player_id(cur, full_name: str, position: str) -> Optional[int]:
    cur.execute(
        "SELECT player_id FROM players WHERE full_name=%s AND position=%s",
        (full_name, position),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def ensure_season_row(cur, player_id: int, season: int):
    cur.execute(
        """
        INSERT INTO player_season_stats (player_id, season)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE season = season
        """,
        (player_id, season),
    )


def update_feature(cur, dest_column: str, player_id: int, season: int, value):
    sql = f"""
        UPDATE player_season_stats
        SET {dest_column} = %s
        WHERE player_id = %s AND season = %s
    """
    cur.execute(sql, (value, player_id, season))


# --------------------------------
# Config for red-zone stats
# --------------------------------
REDZONE_CONFIG = [
    # Receiving red-zone targets
    {
        "name": "Red zone targets",
        "path": "advanced/red-zone/receiving",
        "inside": 20,
        "positions": {"RB", "WR", "TE"},
        "sortBy": "targets",
        "source_field": "targets",
        "dest_column": "red_zone_targets",
        "parser": parse_int,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Red zone target share",
        "path": "advanced/red-zone/receiving",
        "inside": 20,
        "positions": {"RB", "WR", "TE"},
        "sortBy": "targets",
        "source_field": "shareOfTeamRzPassTargets",
        "dest_column": "red_zone_target_share",
        "parser": parse_pct_to_decimal,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Red zone receiving TDs",
        "path": "advanced/red-zone/receiving",
        "inside": 20,
        "positions": {"RB", "WR", "TE"},
        "sortBy": "targets",
        "source_field": "receivingTouchdowns",
        "dest_column": "red_zone_rec_tds",
        "parser": parse_int,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Red zone receiving conversion rate",
        "path": "advanced/red-zone/receiving",
        "inside": 20,
        "positions": {"RB", "WR", "TE"},
        "sortBy": "targets",
        "source_field": "conversionRate",
        "dest_column": "red_zone_rec_conversion_rate",
        "parser": parse_pct_to_decimal,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    # Rushing red-zone TDs
    {
        "name": "Red zone rushing TDs",
        "path": "advanced/red-zone/rushing",
        "inside": 20,
        "positions": {"QB", "RB"},
        "sortBy": "rushingAttempts",
        "source_field": "rushingTouchdowns",
        "dest_column": "red_zone_rush_tds",
        "parser": parse_int,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
]


def run_for_redzone_stat(cur, season: int, cfg: dict):
    full_url = f"{BASE}/{cfg['path']}"
    updated = 0
    missing = 0
    skipped_pos = 0

    for pos_filter in sorted(cfg["positions"]):
        params = {
            "season": season,
            "inside": cfg["inside"],
            "position": pos_filter,
            "sortBy": cfg["sortBy"],
            "pageSize": cfg.get("pageSize", 100),
            **cfg.get("extra_params", {}),
        }

        rows = fetch_all_rows(full_url, params)

        for row in rows:
            name = row.get("playerName")
            pos = row.get("position")

            if not name or not pos:
                continue

            if pos not in cfg["positions"]:
                skipped_pos += 1
                continue

            raw_val = row.get(cfg["source_field"])
            val = cfg["parser"](raw_val)
            if val is None:
                continue

            pid = get_player_id(cur, name, pos)
            if pid is None:
                missing += 1
                continue

            ensure_season_row(cur, pid, season)
            update_feature(cur, cfg["dest_column"], pid, season, val)
            updated += 1

    print(
        f"[DONE] {cfg['name']} season={season} "
        f"updated={updated} skipped_pos={skipped_pos} missing_players={missing}"
    )


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Set to empty set() or None to run all
    RUN_ONLY = {
        "red_zone_targets",
        "red_zone_target_share",
        "red_zone_rec_tds",
        "red_zone_rec_conversion_rate",
        "red_zone_rush_tds",
    }

    for cfg in REDZONE_CONFIG:
        if RUN_ONLY and cfg["dest_column"] not in RUN_ONLY:
            continue

        print(f"\n[INFO] Scraping: {cfg['name']} -> {cfg['dest_column']}")
        for season in SEASONS:
            run_for_redzone_stat(cur, season, cfg)
            conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()