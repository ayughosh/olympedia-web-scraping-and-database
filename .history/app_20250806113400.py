import re
import time
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values, Json

# ── CONFIG ────────────────────────────────────────────────────────────────────
COUNTRY_URL      = "https://www.olympedia.org/countries/IND/"
EDITION_BASE     = COUNTRY_URL + "editions/"
ATHLETE_URL_FMT  = "https://www.olympedia.org/athletes/{}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

DB_CONFIG = {
    "dbname":   "your_db",
    "user":     "your_user",
    "password": "your_pass",
    "host":     "localhost",
    "port":     5432,
}

# ── 1) PREPARE DATABASE ───────────────────────────────────────────────────────
conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS athletes (
  athlete_id   INTEGER     PRIMARY KEY,
  used_name    TEXT,
  full_name    TEXT,
  sex          TEXT,
  born         TEXT,
  died         TEXT,
  nationality  TEXT,
  roles        TEXT,
  affiliations TEXT,
  medals_og    JSONB
);
""")
conn.commit()

# ── 2) SCRAPE EDITION IDs FROM COUNTRY PAGE ──────────────────────────────────
def get_edition_ids():
    resp = requests.get(COUNTRY_URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    ids = []
    # Links look like <a href="/countries/IND/editions/8">Results</a>
    for a in soup.select("a[href^='/countries/IND/editions/']"):
        m = re.search(r"/editions/(\d+)", a["href"])
        if m:
            ids.append(int(m.group(1)))
    return sorted(set(ids))

# ── 3) SCRAPE ATHLETE IDs FROM EACH EDITION PAGE ─────────────────────────────
def get_athlete_ids_from_edition(ed_id):
    url = f"{EDITION_BASE}{ed_id}"
    print(f"→ Visiting edition {ed_id}: {url}")
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    ids = set()
    # Athlete links are <a href="/athletes/71385">Name</a>
    for a in soup.select("a[href^='/athletes/']"):
        m = re.match(r"/athletes/(\d+)", a["href"])
        if m:
            ids.add(int(m.group(1)))
    return ids

# ── 4) FETCH & PARSE AN ATHLETE DETAIL PAGE ─────────────────────────────────
def fetch_athlete_details(aid: int) -> dict:
    url = ATHLETE_URL_FMT.format(aid)
    print(f"    Fetching athlete {aid}: {url}")
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Bio info box
    bio = {}
    info = soup.select_one("div.biographical-information")
    for row in info.select("tr"):
        key = row.th.get_text(strip=True).lower().replace(" ", "_")
        val = row.td.get_text(" ", strip=True)
        bio[key] = val

    # Medal count box (OG)
    medals = {}
    for tr in soup.select("table.medals-OG tr"):
        cols = tr.select("th, td")
        if len(cols) == 2:
            medals[cols[0].text.strip()] = int(cols[1].text.strip())

    return {
        "athlete_id":   aid,
        "used_name":    bio.get("used_name", ""),
        "full_name":    bio.get("full_name", ""),
        "sex":          bio.get("sex", ""),
        "born":         bio.get("born", ""),
        "died":         bio.get("died", ""),
        "nationality":  bio.get("nationality", ""),
        "roles":        bio.get("roles", ""),
        "affiliations": bio.get("affiliations", ""),
        "medals_og":    medals,
    }

# ── 5) MAIN WORKFLOW: GATHER, CHECK, SCRAPE, INSERT ─────────────────────────
edition_ids = get_edition_ids()
print("Editions found:", edition_ids)

seen_athletes = set()
new_rows = []

for ed in edition_ids:
    for aid in get_athlete_ids_from_edition(ed):
        if aid in seen_athletes:
            continue
        seen_athletes.add(aid)

        # Skip already-in-DB
        cur.execute("SELECT 1 FROM athletes WHERE athlete_id=%s", (aid,))
        if cur.fetchone():
            continue

        details = fetch_athlete_details(aid)
        new_rows.append((
            details["athlete_id"],
            details["used_name"],
            details["full_name"],
            details["sex"],
            details["born"],
            details["died"],
            details["nationality"],
            details["roles"],
            details["affiliations"],
            Json(details["medals_og"]),
        ))
        time.sleep(0.5)  # be polite

# Bulk insert any new athletes
if new_rows:
    execute_values(cur, """
      INSERT INTO athletes
        (athlete_id, used_name, full_name, sex, born, died,
         nationality, roles, affiliations, medals_og)
      VALUES %s
    """, new_rows)
    conn.commit()
    print(f"Inserted {len(new_rows)} new athletes.")
else:
    print("No new athletes to insert.")

cur.close()
conn.close()
