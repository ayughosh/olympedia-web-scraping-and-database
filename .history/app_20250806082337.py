import time
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values

# 1. DB setup
conn = psycopg2.connect(
    dbname="yourdb", user="you", password="pw", host="localhost", port=5432
)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS indian_athletes (
    athlete_id   INTEGER PRIMARY KEY,
    name         TEXT,
    country_code CHAR(3),
    sport        TEXT,
    first_year   INTEGER,
    last_year    INTEGER,
    total_medals INTEGER
);
""")
conn.commit()

# 2. Scraper settings
BASE_URL = "https://www.olympedia.org/countries/IND/athletes"
HEADERS = {"User-Agent": "YourName (+https://yourdomain)"}

def parse_athletes_from_page(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table.table tbody tr")
    data = []
    for tr in rows:
        # Athlete link has /athletes/<id>
        link = tr.find("a", href=True)
        href = link["href"]
        athlete_id = int(href.split("/")[2])
        name = link.get_text(strip=True)
        cols = tr.find_all("td")
        sport     = cols[1].get_text(strip=True)
        years     = cols[2].get_text(strip=True).split("–")
        first_year = int(years[0])
        last_year  = int(years[-1])
        medals    = cols[3].get_text(strip=True)
        total_medals = int(medals) if medals.isdigit() else 0

        data.append((athlete_id, name, "IND", sport, first_year, last_year, total_medals))
    return data

# 3. Crawl & insert
all_athletes = []
page = 1
while True:
    print(f"Fetching page {page}…")
    resp = requests.get(f"{BASE_URL}?page={page}", headers=HEADERS)
    resp.raise_for_status()
    page_data = parse_athletes_from_page(resp.text)
    if not page_data:
        break
    all_athletes.extend(page_data)
    page += 1
    time.sleep(1)  # be polite

# Bulk insert (upsert on conflict)
sql = """
INSERT INTO indian_athletes
  (athlete_id, name, country_code, sport, first_year, last_year, total_medals)
VALUES %s
ON CONFLICT (athlete_id) DO UPDATE
  SET name = EXCLUDED.name
    , sport = EXCLUDED.sport
    , first_year = EXCLUDED.first_year
    , last_year = EXCLUDED.last_year
    , total_medals = EXCLUDED.total_medals;
"""
execute_values(cur, sql, all_athletes)
conn.commit()
print(f"Inserted/updated {len(all_athletes)} athletes.")
cur.close()
conn.close()
