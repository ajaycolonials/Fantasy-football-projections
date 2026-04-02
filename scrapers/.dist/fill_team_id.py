import requests
import mysql.connector
import os
import re
import time

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
    "statType": "GAMES_PLAYED",
}

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
    "Washington Football Team": 32,
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def normalize_name(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


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


def fetch_search_rows(season: int, player_name: str) -> list[dict]:
    params = {
        **COMMON_PARAMS,
        "season": season,
        "page": 1,
        "playerName": player_name,
    }
    payload = get_json_with_retries(BASE_URL, params)
    return payload.get("data", [])


def get_missing_team_rows(cur):
    cur.execute(
        """
        SELECT
            pss.player_id,
            p.full_name,
            p.position,
            pss.season,
            pss.games_played
        FROM player_season_stats pss
        JOIN players p
            ON p.player_id = pss.player_id
        WHERE pss.team_id IS NULL
          AND p.position IN ('QB', 'RB', 'WR', 'TE')
        ORDER BY pss.season, p.position, p.full_name
        """
    )
    return cur.fetchall()


def update_team_id(cur, player_id: int, season: int, team_id: int):
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


def choose_best_match(rows: list[dict], target_name: str, target_pos: str):
    target_norm = normalize_name(target_name)

    exact_matches = []
    fuzzy_matches = []

    for row in rows:
        api_name = row.get("playerName")
        api_pos = row.get("position")

        if not api_name or not api_pos:
            continue

        api_norm = normalize_name(api_name)

        if api_pos == target_pos and api_name == target_name:
            exact_matches.append(row)
        elif api_pos == target_pos and api_norm == target_norm:
            fuzzy_matches.append(row)

    if exact_matches:
        return exact_matches[0], "exact"
    if fuzzy_matches:
        return fuzzy_matches[0], "normalized"

    return None, None


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    missing_rows = get_missing_team_rows(cur)
    print(f"[INFO] Missing team_id rows to process: {len(missing_rows)}")

    updated = 0
    no_api_rows = 0
    no_match_found = 0
    missing_team_name = 0
    unmapped_team_name = 0
    multi_result_cases = 0

    for idx, (player_id, full_name, position, season, games_played) in enumerate(missing_rows, start=1):
        print(f"\n[{idx}/{len(missing_rows)}] {full_name} | {position} | {season}")

        try:
            rows = fetch_search_rows(season, full_name)
        except Exception as e:
            print(f"[ERROR] API request failed: {e}")
            continue

        if not rows:
            no_api_rows += 1
            print("[MISS] No API rows returned")
            continue

        if len(rows) > 1:
            multi_result_cases += 1
            print(f"[INFO] API returned {len(rows)} rows")

        best_row, match_type = choose_best_match(rows, full_name, position)

        if best_row is None:
            no_match_found += 1
            print("[MISS] No exact/normalized same-position match found")
            print("[DEBUG] Returned rows:")
            for row in rows[:10]:
                print(
                    f"  name={row.get('playerName')!r}, "
                    f"pos={row.get('position')!r}, "
                    f"team={row.get('teamName')!r}"
                )
            continue

        team_name = best_row.get("teamName")
        if not team_name:
            missing_team_name += 1
            print(f"[MISS] Match found ({match_type}) but teamName missing")
            print("[DEBUG] Best row:", best_row)
            continue

        team_id = TEAM_NAME_TO_ID.get(team_name)
        if team_id is None:
            unmapped_team_name += 1
            print(f"[MISS] Unmapped team name: {team_name}")
            print("[DEBUG] Best row:", best_row)
            continue

        rowcount = update_team_id(cur, player_id, season, team_id)
        updated += rowcount

        print(
            f"[OK] match_type={match_type} "
            f"team_name={team_name!r} "
            f"team_id={team_id} "
            f"updated_rows={rowcount}"
        )

        if idx % 25 == 0:
            conn.commit()
            print("[INFO] Intermediate commit")

    conn.commit()
    cur.close()
    conn.close()

    print("\n[SUMMARY]")
    print("updated:", updated)
    print("no_api_rows:", no_api_rows)
    print("no_match_found:", no_match_found)
    print("missing_team_name:", missing_team_name)
    print("unmapped_team_name:", unmapped_team_name)
    print("multi_result_cases:", multi_result_cases)


if __name__ == "__main__":
    main()