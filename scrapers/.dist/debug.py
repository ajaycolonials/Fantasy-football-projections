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
TARGET_SEASON = 2022
TARGET_NAME = "Aaron Rodgers"
TARGET_POS = "QB"

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

def main():
    print(f"[INFO] Fetching API rows for season {TARGET_SEASON}")
    rows = fetch_all_rows(TARGET_SEASON)
    print(f"[INFO] Total API rows fetched: {len(rows)}")

    api_match = None
    close_name_rows = []

    for row in rows:
        player_name = row.get("playerName")
        pos = row.get("position")

        if TARGET_NAME.lower().replace(".", "") in str(player_name).lower().replace(".", ""):
            close_name_rows.append(row)

        if player_name == TARGET_NAME and pos == TARGET_POS:
            api_match = row
            break

    print("\n================ API CHECK ================")
    if api_match:
        print("[FOUND EXACT API MATCH]")
        print(api_match)
        team_name = api_match.get("teamName")
        print("API teamName:", team_name)
        print("Mapped internal team_id:", TEAM_NAME_TO_ID.get(team_name))
    else:
        print("[NO EXACT API MATCH FOUND]")
        print("Closest API rows:")
        for row in close_name_rows[:10]:
            print(row)

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("\n================ PLAYERS TABLE CHECK ================")
    cur.execute(
        """
        SELECT player_id, full_name, position, team_id,
               statrankings_player_page_id, statrankings_api_player_id
        FROM players
        WHERE full_name = %s AND position = %s
        """,
        (TARGET_NAME, TARGET_POS),
    )
    exact_player_rows = cur.fetchall()

    if exact_player_rows:
        print("[EXACT DB PLAYER MATCHES]")
        for r in exact_player_rows:
            print(r)
    else:
        print("[NO EXACT DB PLAYER MATCH]")
        cur.execute(
            """
            SELECT player_id, full_name, position, team_id,
                   statrankings_player_page_id, statrankings_api_player_id
            FROM players
            WHERE full_name LIKE %s
            ORDER BY full_name
            """,
            ("%Amon%",),
        )
        fuzzy_player_rows = cur.fetchall()
        print("[CLOSE DB PLAYER ROWS]")
        for r in fuzzy_player_rows:
            print(r)

    print("\n================ PLAYER_SEASON_STATS CHECK ================")
    cur.execute(
        """
        SELECT
            p.player_id,
            p.full_name,
            p.position,
            pss.season,
            pss.games_played,
            pss.team_id
        FROM player_season_stats pss
        JOIN players p ON p.player_id = pss.player_id
        WHERE pss.season = %s
          AND p.position = %s
          AND p.full_name LIKE %s
        ORDER BY p.full_name
        """,
        (TARGET_SEASON, TARGET_POS, "%Amon%"),
    )
    pss_rows = cur.fetchall()

    if pss_rows:
        print("[MATCHING PLAYER_SEASON_STATS ROWS]")
        for r in pss_rows:
            print(r)
    else:
        print("[NO PLAYER_SEASON_STATS ROWS FOUND FOR FUZZY NAME SEARCH]")

    if exact_player_rows:
        exact_player_id = exact_player_rows[0][0]

        print("\n================ TARGETED UPDATE ELIGIBILITY CHECK ================")
        cur.execute(
            """
            SELECT player_id, season, games_played, team_id
            FROM player_season_stats
            WHERE player_id = %s
              AND season = %s
            """,
            (exact_player_id, TARGET_SEASON),
        )
        exact_pss = cur.fetchone()
        print("Exact player_season_stats row:", exact_pss)

        if api_match:
            mapped_team_id = TEAM_NAME_TO_ID.get(api_match.get("teamName"))
            print("Would attempt update with team_id:", mapped_team_id)

            cur.execute(
                """
                SELECT COUNT(*)
                FROM player_season_stats
                WHERE player_id = %s
                  AND season = %s
                  AND team_id IS NULL
                """,
                (exact_player_id, TARGET_SEASON),
            )
            eligible = cur.fetchone()[0]
            print("Rows eligible for update_team_id_only:", eligible)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()