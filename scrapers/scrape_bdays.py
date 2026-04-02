import re
import time
import requests
import mysql.connector
from datetime import datetime
from mysql.connector import Error

import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

PLAYER_PAGE_URL = "https://statrankings.com/nfl/player-pages/{page_id}/{slug}"

# Example match: 6/7/1996 (Age 29)
DOB_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\s*\(Age\s*\d+\)", re.IGNORECASE)


def slugify(full_name: str) -> str:
    s = full_name.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)   # remove punctuation
    s = re.sub(r"\s+", "-", s)       # spaces -> hyphens
    return s


def parse_mmddyyyy(s: str):
    return datetime.strptime(s, "%m/%d/%Y").date()


def fetch_birthdate(page_id: int, full_name: str):
    slug = slugify(full_name)
    url = PLAYER_PAGE_URL.format(page_id=page_id, slug=slug)

    r = SESSION.get(url, timeout=20)
    if r.status_code != 200:
        return None

    m = DOB_RE.search(r.text)
    if not m:
        return None

    return parse_mmddyyyy(m.group(1))


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT player_id, full_name, statrankings_player_page_id
        FROM players
        WHERE statrankings_player_page_id IS NOT NULL
          AND birthdate IS NULL
    """)
    rows = cur.fetchall()

    print(f"[INFO] Players missing birthdate: {len(rows)}")

    updated = 0
    failed = 0

    for i, (player_id, full_name, page_id) in enumerate(rows, start=1):
        bd = None
        try:
            bd = fetch_birthdate(int(page_id), full_name)
        except requests.RequestException:
            bd = None

        if bd is None:
            failed += 1
        else:
            cur.execute(
                "UPDATE players SET birthdate=%s WHERE player_id=%s",
                (bd, int(player_id))
            )
            updated += 1

        if i % 50 == 0:
            conn.commit()
            print(f"[PROGRESS] {i}/{len(rows)} updated={updated} failed={failed}")

        time.sleep(0.15)

    conn.commit()
    cur.close()
    conn.close()

    print(f"[DONE] Updated birthdates: {updated}")
    print(f"[DONE] Failed parses: {failed}")


if __name__ == "__main__":
    try:
        main()
    except Error as db_err:
        print(f"[DB ERROR] {db_err}")
    except requests.RequestException as req_err:
        print(f"[HTTP ERROR] {req_err}")
    except Exception as e:
        print(f"[ERROR] {e}")