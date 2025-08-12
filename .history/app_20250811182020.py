import re
import time
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values, Json

# ── CONFIG ───────────────────────────────────────────────────────────────────
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
    "dbname":   "olympic_data",
    "user":     "postgres",
    "password": "Ayushi11",
    "host":     "localhost",
    "port":     5432,
}
def ensure_events_table(cur, conn):
    # Is the table already there (in public)?
    cur.execute("SELECT to_regclass('public.events');")
    exists = cur.fetchone()[0]

    if exists is None:
        print("public.events not found; creating...")
        cur.execute("""
            CREATE TABLE public.events (
                event_id   SERIAL PRIMARY KEY,
                athlete_id INTEGER NOT NULL
                    REFERENCES public.athletes(athlete_id)
                    ON DELETE CASCADE,
                games      TEXT NOT NULL,
                discipline TEXT NOT NULL,
                team       TEXT,
                pos        TEXT,
                medal      TEXT,
                used_as    TEXT
            );
        """)
        conn.commit()
        print("Created public.events")
    else:
        print("public.events already exists")

# ── 1) CONNECT & PREPARE TABLE ────────────────────────────────────────────────
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
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    event_id     SERIAL      PRIMARY KEY,
    athlete_id   INTEGER     NOT NULL
      REFERENCES athletes(athlete_id)
      ON DELETE CASCADE,
    games        TEXT        NOT NULL,
    discipline   TEXT        NOT NULL,
    team         TEXT,
    pos          TEXT,
    medal        TEXT,
    used_as      TEXT
  );
""")
conn.commit()

def clean_name(s: str) -> str:
    if not s:
        return ""
    # remove middle-dots and periods, then collapse spaces
    s = re.sub(r"[•·‧\.]+", " ", s)   # covers • (U+2022), · (U+00B7), ‧ (U+2027), .
    s = re.sub(r"\s+", " ", s).strip()
    return s
# ── 2) GET ALL EDITION IDs ────────────────────────────────────────────────────
def get_edition_ids():
    r = requests.get(COUNTRY_URL, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    ids = {
      int(m.group(1))
      for a in soup.select("a[href^='/countries/IND/editions/']")
      if (m := re.search(r"/editions/(\d+)", a["href"]))
    }
    return sorted(ids)

# ── 3) GET ATHLETE IDs FROM AN EDITION ───────────────────────────────────────
def get_athlete_ids_from_edition(ed_id):
    url = f"{EDITION_BASE}{ed_id}"
    print(f"→ Edition {ed_id}: {url}")
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return {
      int(m.group(1))
      for a in soup.select("a[href^='/athletes/']")
      if (m := re.match(r"/athletes/(\d+)", a["href"]))
    }

# ── 4) FETCH & PARSE ATHLETE DETAILS ─────────────────────────────────────────
def fetch_athlete_details(aid):
    r = requests.get(ATHLETE_URL_FMT.format(aid), headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Locate the bio table and fill bio dict
    bio = {}
    for tbl in soup.find_all("table"):
        ths = [th.get_text(strip=True) for th in tbl.select("th")]
        if ths and ths[0] == "Roles" and "Sex" in ths:
            for row in tbl.select("tr"):
                th = row.find("th")
                td = row.find("td")
                key = th.get_text(strip=True).lower().replace(" ", "_")
                if key == "noc":
                    a = td.find("a")
                    val = a.get_text(strip=True) if a else td.get_text(" ", strip=True)
                else:
                    val = td.get_text(" ", strip=True)
                bio[key] = val
            break
    # Use NOC as nationality if there is no separate nationality row
    nationality_val = bio.get("nationality") or bio.get("noc", "")

    # Parse medals table (handle both class forms)
    medals = {}
    medals_tbl = soup.select_one("table.medals-OG, table.medals.OG")
    if medals_tbl:
        for tr in medals_tbl.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                label = th.get_text(strip=True).capitalize()
                match = re.search(r"\d+", td.get_text())
                if match:
                    medals[label] = int(match.group())

    # Parse events/results (use hdr.find_next_sibling)
    events = []
    hdr = soup.find("h2", string=lambda t: t and "Results" in t)
    res_tbl = hdr.find_next_sibling("table") if hdr else None
    if res_tbl:
        for tr in res_tbl.select("tbody tr"):
            cols = tr.select("td")
            if len(cols) >= 6:
                events.append({
                    "games": cols[0].get_text(" ", strip=True),
                    "event": cols[1].get_text(" ", strip=True),
                    "team":  cols[2].get_text(" ", strip=True),
                    "pos":   cols[3].get_text(" ", strip=True),
                    "medal": cols[4].get_text(" ", strip=True),
                    "as":    cols[5].get_text(" ", strip=True),
                })

    return {
        "athlete_id": aid,
        "used_name":  bio.get("used_name", ""),
        "full_name":  bio.get("full_name", ""),
        "sex":        bio.get("sex", ""),
        "born":       bio.get("born", ""),
        "died":       bio.get("died", ""),
        "nationality": nationality_val,
        "roles":      bio.get("roles", ""),
        "affiliations": bio.get("affiliations", ""),
        "medals_og":  medals,
        "events":     events,
    }


# ── 5) MAIN LOOP: COLLECT, CHECK DB, SCRAPE, BULK INSERT ────────────────────
edition_ids = get_edition_ids()
print("Found editions:", edition_ids)

seen = set()
athlete_rows = []
event_rows=[]

for ed in edition_ids:
    for aid in get_athlete_ids_from_edition(ed):
        if aid in seen:
            continue
        seen.add(aid)

        # ← DB check comes *before* any network call to athlete details
        cur.execute("SELECT 1 FROM athletes WHERE athlete_id=%s", (aid,))
        if cur.fetchone():
            print(f"→ Athlete {aid} already in DB, skipping.")
            continue

        details = fetch_athlete_details(aid)
        athlete_rows.append((
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
        
        # 2) many rows for the events table
        for evt in details["events"]:
            event_rows.append((
            details["athlete_id"],      # or aid
            evt["games"],
            evt["event"],               # maps to events.discipline
            evt["team"],
            evt["pos"],
            evt["medal"],
            evt["as"],
        ))

        time.sleep(0.5)
    

if athlete_rows:
    execute_values(cur, """
        INSERT INTO athletes
        (athlete_id, used_name, full_name, sex, born, died,
        nationality, roles, affiliations, medals_og)
        VALUES %s
    """,   athlete_rows)
    conn.commit()
    
        # now also insert their events

if event_rows:
    execute_values(cur, """
        INSERT INTO public.events
        (athlete_id, games, discipline, team, pos, medal, used_as)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, event_rows)
    conn.commit()

    print(f"Inserted {len(athlete_rows)} new athletes.")
    cur.execute("SELECT COUNT(*) FROM athletes")
    print("Total in DB now:", cur.fetchone()[0])
else:
    print("No new athletes to insert.")

cur.close()
conn.close()
