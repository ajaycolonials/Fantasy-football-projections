import requests
import mysql.connector
from mysql.connector import Error

# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Box60Boat",
    "database": "fantasy_app"
}

SEASONS = [2021, 2022, 2023, 2024, 2025]
SEASON_TYPE = "REG"
BASE_URL = "https://api.statrankings.com/stats-service/nfl/teams"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# =========================
# TEAM NAME -> INTERNAL team_id
# FILL THESE IDS TO MATCH YOUR teams TABLE
# =========================
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

# =========================
# STAT MAPPINGS
# format:
# (endpoint_group, stat_type, db_column)
#
# fill the placeholders once you confirm them
# =========================
STAT_MAPPINGS = [
    # confirmed / partially confirmed
    ("scoring-offense", "AVERAGE_SCORING_MARGIN", "average_scoring_margin"),
    ("scoring-offense", "OFFENSIVE_TOUCHDOWNS_PER_GAME", "offensive_tds_per_game"),
    ("scoring-offense", "POINTS_PER_GAME", "points_per_game"),
    ("scoring-offense", "POINTS_PER_PLAY", "points_per_play"),

    ("total-offense", "PLAYS_PER_GAME", "plays_per_game"),
    ("total-offense", "SECONDS_PER_PLAY", "seconds_per_play"),
    ("advanced/pace-and-play-calling", "NEUTRAL_SITUATION_PACE", "neutral_situation_pace"),  # API spelling
    ("advanced/pace-and-play-calling", "NO_HUDDLE_RATE", "no_huddle_rate"),

    ("total-offense", "YARDS_PER_PLAY", "yards_per_play"),
    ("total-offense", "YARDS_PER_GAME", "yards_per_game"),

    ("total-offense", "FIRST_DOWNS_PER_GAME", "first_downs_per_game"),
    ("total-offense", "FIRST_DOWNS_PER_PLAY", "first_downs_per_play"),
    ("total-offense", "THIRD_DOWN_CONVERSION_PERCENTAGE", "third_down_conversion_pct"),
    ("total-offense", "FOURTH_DOWN_CONVERSION_PERCENTAGE", "fourth_down_conversion_pct"),

    # placeholders to fill in later
    ("passing-offense", "PASS_ATTEMPTS_PER_GAME", "pass_attempts_per_game"),
    ("passing-offense", "PASSING_PLAY_PERCENTAGE", "passing_play_pct"),
    ("passing-offense", "PASSING_TOUCHDOWNS_PER_GAME", "passing_tds_per_game"),
    ("passing-offense", "COMPLETIONS_PER_GAME", "completions_per_game"),
    ("passing-offense", "YARDS_PER_COMPLETION", "yards_per_completion"),
    ("advanced/passing", "PASS_RATE_OVER_EXPECTED", "pass_rate_over_expectation"),
    ("advanced/pace-and-play-calling", "PASS_RATE_IN_NEUTRAL_GAME_SCRIPT", "pass_rate_neutral_script"),
    ("advanced/pace-and-play-calling", "PASS_RATE_IN_POSITIVE_GAME_SCRIPT", "pass_rate_positive_script"),
    ("advanced/pace-and-play-calling", "PASS_RATE_IN_NEGATIVE_GAME_SCRIPT", "pass_rate_negative_script"),
    ("advanced/pace-and-play-calling", "PASS_RATE_RED_ZONE", "pass_rate_red_zone"),

    ("rushing-offense", "RUSHING_ATTEMPTS_PER_GAME", "rushing_attempts_per_game"),
    ("rushing-offense", "RUSHING_FIRST_DOWN_PERCENTAGE", "rushing_first_down_pct"),
    ("rushing-offense", "RUSHING_FIRST_DOWNS_PER_GAME", "rushing_first_downs_per_game"),
    ("rushing-offense", "RUSHING_PLAY_PERCENTAGE", "rushing_play_pct"),
    ("rushing-offense", "RUSHING_TOUCHDOWNS_PER_GAME", "rushing_tds_per_game"),
    ("rushing-offense", "RUSHING_YARDS_PER_GAME", "rushing_yards_per_game"),
    ("rushing-offense", "YARDS_PER_RUSH_ATTEMPT", "rush_yards_per_attempt"),
    ("advanced/pace-and-play-calling", "RUSH_RATE_IN_NEUTRAL_GAME_SCRIPT", "rush_rate_neutral_script"),
    ("advanced/pace-and-play-calling", "RUSH_RATE_IN_POSITIVE_GAME_SCRIPT", "rush_rate_positive_script"),
    ("advanced/pace-and-play-calling", "RUSH_RATE_IN_NEGATIVE_GAME_SCRIPT", "rush_rate_negative_script"),

    ("advanced/passing", "EXPLOSIVE_PLAY_RATE", "explosive_play_rate"),
    ("advanced/epa", "EPA_PER_PLAY_PASSING", "epa_per_play_pass"),
    ("advanced/epa", "EPA_PER_PLAY_RUSHING", "epa_per_play_rush"),

    ("advanced/rushing", "INSIDE_ZONE_RATE", "inside_zone_rate"),
    ("advanced/rushing", "INSIDE_ZONE_YARDS_PER_ATTEMPT", "inside_zone_ypa"),
    ("advanced/rushing", "OUTSIDE_ZONE_RATE", "outside_zone_rate"),
    ("advanced/rushing", "OUTSIDE_ZONE_YARDS_PER_ATTEMPT", "outside_zone_ypa"),
    ("advanced/rushing", "POWER_RATE", "power_rate"),
    ("advanced/rushing", "POWER_YARDS_PER_ATTEMPT", "power_ypa"),
    ("advanced/trench-play", "STUFF_RATE", "stuff_rate"),

    ("advanced/rushing", "AVOIDED_TACKLE_RATE", "avoided_tackle_rate"),
    ("advanced/rushing", "YARDS_AFTER_CONTACT_PER_ATTEMPT", "yac_per_attempt"),
    ("advanced/rushing", "YARDS_BEFORE_CONTACT_PER_ATTEMPT", "ybc_per_attempt"),
]

# =========================
# DB HELPERS
# =========================
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def ensure_team_season_row(conn, team_id, season):
    query = """
        INSERT INTO team_season_stats (team_id, season)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE season = VALUES(season)
    """
    cursor = conn.cursor()
    cursor.execute(query, (team_id, season))
    conn.commit()
    cursor.close()

def update_team_stat(conn, team_id, season, column_name, value):
    query = f"""
        UPDATE team_season_stats
        SET {column_name} = %s
        WHERE team_id = %s AND season = %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (value, team_id, season))
    conn.commit()
    cursor.close()

# =========================
# API HELPERS
# =========================
def build_url(endpoint_group, season, stat_type):
    return (
        f"{BASE_URL}/{endpoint_group}"
        f"?season={season}"
        f"&statType={stat_type}"
        f"&sortOrder=ASC"
        f"&seasonType={SEASON_TYPE}"
    )

def parse_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def fetch_stat_rows(endpoint_group, season, stat_type):
    url = build_url(endpoint_group, season, stat_type)
    print(f"Fetching: {url}")

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        if "results" in data and isinstance(data["results"], list):
            return data["results"]
        return [data]

    return []

# =========================
# MAIN LOAD LOGIC
# =========================
def load_team_season_stats():
    conn = None
    try:
        conn = get_connection()

        print(f"Loaded {len(TEAM_NAME_TO_ID)} team name mappings")

        for season in SEASONS:
            print(f"\n=== SEASON {season} ===")

            for endpoint_group, stat_type, db_column in STAT_MAPPINGS:
                if endpoint_group == "FILL_ENDPOINT_GROUP" or stat_type == "FILL_STATTYPE":
                    print(f"Skipping placeholder mapping for column: {db_column}")
                    continue

                try:
                    rows = fetch_stat_rows(endpoint_group, season, stat_type)
                except Exception as e:
                    print(f"Failed fetching {db_column} for {season}: {e}")
                    continue

                updated_count = 0
                missing_team_count = 0

                for row in rows:
                    team_name = row.get("teamName")
                    current_season_value = parse_float(row.get("currentSeason"))

                    if not team_name:
                        continue

                    internal_team_id = TEAM_NAME_TO_ID.get(team_name)

                    if internal_team_id is None:
                        missing_team_count += 1
                        print(f"Unmapped team name: {team_name}")
                        continue

                    ensure_team_season_row(conn, internal_team_id, season)
                    update_team_stat(conn, internal_team_id, season, db_column, current_season_value)
                    updated_count += 1

                print(
                    f"{db_column}: updated {updated_count} rows"
                    f" | missing team mappings: {missing_team_count}"
                )

        print("\nDone loading team_season_stats.")

    except Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"General error: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    load_team_season_stats()