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

TEAM_NAME_TO_ID = {
    "Arizona Cardinals": 1,
    "Atlanta Falcons": 2,
    "Baltimore Ravens": 3,
    "Buffalo Bills": 4,
    "Carolina Panthers": 5,
    "Chicago Bears": 6,
    "Cincinnati Bengals": 7,
    "Cleveland Browns": 8,
    "Dallas Cowboys": 9,
    "Denver Broncos": 10,
    "Detroit Lions": 11,
    "Green Bay Packers": 12,
    "Houston Texans": 13,
    "Indianapolis Colts": 14,
    "Jacksonville Jaguars": 15,
    "Kansas City Chiefs": 16,
    "Las Vegas Raiders": 17,
    "Los Angeles Chargers": 18,
    "Los Angeles Rams": 19,
    "Miami Dolphins": 20,
    "Minnesota Vikings": 21,
    "New England Patriots": 22,
    "New Orleans Saints": 23,
    "New York Giants": 24,
    "New York Jets": 25,
    "Philadelphia Eagles": 26,
    "Pittsburgh Steelers": 27,
    "San Francisco 49ers": 28,
    "Seattle Seahawks": 29,
    "Tampa Bay Buccaneers": 30,
    "Tennessee Titans": 31,
    "Washington Commanders": 32,
}

def fetch_all_rows(season: int) -> list[dict]:
    all_rows = []
    page = 1

    while True:
        params = {
            **COMMON_PARAMS,
            "season": season,
            "statType": STAT_TYPE,
            "page": page,
        }
        r = requests.get(
            BASE_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
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

def update_team_id_only(cur, player_id: int, season: int, team_id: int):
    cur.execute(
        """
        UPDATE player_season_stats
        SET team_id = %s
        WHERE player_id = %s
          AND season = %s
          AND team_id IS NULL
        """,
        (team_id, player_id, season),
    )
    return cur.rowcount

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    for season in [2021, 2022, 2023, 2024, 2025]:
        print(f"[INFO] Backfilling team_id for season {season}")
        rows = fetch_all_rows(season)

        updated = 0
        skipped_pos = 0
        missing_players = 0
        missing_team_names = 0
        unmapped_team_names = 0

        for row in rows:
            name = row.get("playerName")
            pos = row.get("position")

            if not name or not pos:
                continue

            if pos not in ALLOWED_POS:
                skipped_pos += 1
                continue

            team_name = row.get("teamName")
            if not team_name:
                missing_team_names += 1
                continue

            team_id = TEAM_NAME_TO_ID.get(team_name)
            if team_id is None:
                unmapped_team_names += 1
                print(f"[WARN] Unmapped team name: {team_name}")
                continue

            pid = get_player_id(cur, name, pos)
            if not pid:
                missing_players += 1
                continue

            updated += update_team_id_only(cur, pid, season, team_id)

        conn.commit()
        print(
            f"[DONE] season={season} updated={updated} "
            f"skipped_non_fantasy_pos={skipped_pos} "
            f"missing_players={missing_players} "
            f"missing_team_names={missing_team_names} "
            f"unmapped_team_names={unmapped_team_names}"
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()