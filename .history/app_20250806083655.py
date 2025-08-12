import time
import requests
import psycopg2
from psycopg2.extras import execute_values

# ── 1. Fetch JSON data ────────────────────────────────────────────────────────
API     = "https://www.olympedia.org/countries/IND/athletes/"
HEADERS = {"User-Agent": "YourName (+https://yourdomain)"}

all_athletes = []
page = 1
while True:
    r = requests.get(API, params={"page": page}, headers=HEADERS)
    r.raise_for_status()
    js = r.json()
    rows = js.get("data", [])
    if not rows:
        break

    for item in rows:
        all_athletes.append((
            item["id"],
            item["name"],
            "IND",
            item["sport"],
            item["first_appearance"],
            item["last_appearance"],
            item["medal_count"],
        ))
    page += 1
    time.sleep(0.5)

# ── 2. Write to Postgres ──────────────────────────────────────────────────────
# Update these credentials to match your setup
DB_PARAMS = {
    "dbname":   "olympic_data",
    "user":     "postgres",
    "password": "Ayushi11",
    "host":     "localhost",
    "port":     5432,
}

# 2a. Connect & ensure table exists
conn = psycopg2.connect(**DB_PARAMS)
cur  = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS indian_athletes (
    athlete_id   INTEGER PRIMARY KEY,
    name         TEXT       NOT NULL,
    country_code CHAR(3)    NOT NULL,
    sport        TEXT,
    first_year   INTEGER,
    last_year    INTEGER,
    total_medals INTEGER
);
""")
conn.commit()

# 2b. Bulk upsert using execute_values
upsert_sql = """
INSERT INTO indian_athletes
  (athlete_id, name, country_code, sport, first_year, last_year, total_medals)
VALUES %s
ON CONFLICT (athlete_id) DO UPDATE
  SET name         = EXCLUDED.name,
      sport        = EXCLUDED.sport,
      first_year   = EXCLUDED.first_year,
      last_year    = EXCLUDED.last_year,
      total_medals = EXCLUDED.total_medals;
"""

execute_values(cur, upsert_sql, all_athletes)
conn.commit()

print(f"Inserted/updated {len(all_athletes)} athletes.")

cur.close()
conn.close()
