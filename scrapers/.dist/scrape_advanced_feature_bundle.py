import requests
import mysql.connector
from typing import Callable, Optional

import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}
ALLOWED_POS = {"QB", "RB", "WR", "TE"}

BASE = "https://api.statrankings.com/stats-service/nfl/players"


def parse_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fetch_all_rows(full_url: str, params: dict) -> list[dict]:
    """Paginate until API returns empty data list."""
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


def update_feature(cur, dest_column: str, player_id: int, season: int, value: float):
    # dest_column must come from the config list below (trusted list)
    sql = f"""
        UPDATE player_season_stats
        SET {dest_column} = %s
        WHERE player_id = %s AND season = %s
    """
    cur.execute(sql, (value, player_id, season))


# ----------------------------
# CONFIG: your new features
# ----------------------------
# For each stat, fill in:
# - path: URL path after /nfl/players (example: "advanced/usage", "advanced/passing", etc)
# - statType: EXACT statType from the network request
# - value_field: which JSON field contains the value (common: "currentSeason" or "statValue")
# - dest_column: the column you added to player_season_stats
#
# FIRST_READ_TARGET_SHARE is filled out because you provided it.
STAT_CONFIG = [
    {
        "name": "First read target share",
        "path": "advanced/usage",
        "statType": "FIRST_READ_TARGET_SHARE",
        "value_field": "currentSeason",
        "dest_column": "first_read_target_share_pct",
        "parser": parse_float,   # stored as percent (0-100)
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },

    # ---- Fill these once you find their network URL + statType ----
    {
        "name": "Snap share",
        "path": "advanced/usage",
        "statType": "SNAP_SHARE",
        "value_field": "currentSeason",
        "dest_column": "snap_share_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "RB opportunity share",
        "path": "advanced/usage",
        "statType": "OPPORTUNITY_SHARE",
        "value_field": "currentSeason",
        "dest_column": "rb_opportunity_share_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "True target share",
        "path": "advanced/usage",
        "statType": "TARGET_RATE",
        "value_field": "currentSeason",
        "dest_column": "true_target_share_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },

    # QB advanced metrics (you’ll paste correct path/statType/value_field from Network)
    {
        "name": "Adj net yards per attempt",
        "path": "advanced/advanced-qb",   # <-- maybe different; paste from Network
        "statType": "ANY_A",
        "value_field": "currentSeason",   # <-- could be currentSeason; paste from Network
        "dest_column": "adj_net_yards_per_att",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Accuracy %",
        "path": "advanced/advanced-qb",
        "statType": "ACCURACY_PERCENTAGE",
        "value_field": "currentSeason",
        "dest_column": "accuracy_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Checkdown %",
        "path": "advanced/advanced-qb",
        "statType": "CHECKDOWN_PERCENTAGE",
        "value_field": "currentSeason",
        "dest_column": "checkdown_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Deep throw rate %",
        "path": "advanced/advanced-qb",
        "statType": "DEEP_THROW_RATE",
        "value_field": "currentSeason",
        "dest_column": "deep_throw_rate_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "First read %",
        "path": "advanced/advanced-qb",
        "statType": "FIRST_READ_PERCENTAGE",
        "value_field": "currentSeason",
        "dest_column": "first_read_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Offensive pressure rate %",
        "path": "advanced/advanced-qb",
        "statType": "OFFENSIVE_PRESSURE_RATE",
        "value_field": "currentSeason",
        "dest_column": "offensive_pressure_rate_pct",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "Time to throw (sec)",
        "path": "advanced/advanced-qb",
        "statType": "TIME_TO_THROW",
        "value_field": "currentSeason",
        "dest_column": "time_to_throw_sec",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "QBR under pressure",
        "path": "advanced/advanced-qb",
        "statType": "QB_RATING_UNDER_PRESSURE",
        "value_field": "currentSeason",
        "dest_column": "qbr_under_pressure",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "QBR when clean",
        "path": "advanced/advanced-qb",
        "statType": "QB_RATING_CLEAN",
        "value_field": "currentSeason",
        "dest_column": "qbr_when_clean",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "ANY/A under pressure",
        "path": "advanced/advanced-qb",
        "statType": "ANY_A_UNDER_PRESSURE",
        "value_field": "currentSeason",
        "dest_column": "anya_under_pressure",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
    {
        "name": "ANY/A when clean",
        "path": "advanced/advanced-qb",
        "statType": "ANY_A_CLEAN",
        "value_field": "currentSeason",
        "dest_column": "anya_when_clean",
        "parser": parse_float,
        "pageSize": 100,
        "extra_params": {"sortOrder": "DESC", "seasonType": "REG"},
    },
]


def run_for_stat(cur, season: int, cfg: dict):
    full_url = f"{BASE}/{cfg['path']}"
    params = {
        "season": season,
        "statType": cfg["statType"],
        "pageSize": cfg.get("pageSize", 100),
        **cfg.get("extra_params", {}),
    }

    rows = fetch_all_rows(full_url, params)

    updated = 0
    skipped_pos = 0
    missing = 0

    for row in rows:
        name = row.get("playerName")
        pos = row.get("position")

        if not name or not pos:
            continue
        if pos not in ALLOWED_POS:
            skipped_pos += 1
            continue

        raw_val = row.get(cfg["value_field"])
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

    print(f"[DONE] {cfg['name']} season={season} updated={updated} skipped_pos={skipped_pos} missing_players={missing}")


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    seasons = [2021, 2022, 2023, 2024, 2025]

    for cfg in STAT_CONFIG:
        # Skip any config you haven't filled in yet

        print(f"\n[INFO] Scraping: {cfg['name']} -> {cfg['dest_column']}")
        for season in seasons:
            run_for_stat(cur, season, cfg)
            conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
