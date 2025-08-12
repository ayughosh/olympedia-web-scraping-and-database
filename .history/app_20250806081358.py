from flask import Flask, jsonify, request
import requests
from bs4 import BeautifulSoup
import time
import re
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

app = Flask(__name__)

# Base URL for Olympedia
BASE_URL = "https://www.olympedia.org"

# --- PostgreSQL Database Configuration ---
# IMPORTANT: Replace these with your actual PostgreSQL credentials
DB_HOST = "localhost"
DB_NAME = "olympic_data"
DB_USER = "postgres" # e.g., "postgres"
DB_PASSWORD = "Ayushi11" # e.g., "password"
DB_PORT="5432"

def connect_db():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
            # port=DB_PORT # You can uncomment this if your PostgreSQL is on a non-default port
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_tables():
    """Creates necessary tables in the PostgreSQL database if they don't exist."""
    conn = connect_db()
    if conn:
        try:
            cur = conn.cursor()
            # Create athletes table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS athletes (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    sex VARCHAR(10),
                    born VARCHAR(255),
                    died VARCHAR(255),
                    height_cm INTEGER,
                    weight_kg INTEGER,
                    noc VARCHAR(10)
                );
            """)
            # Create olympic_results table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS olympic_results (
                    result_id SERIAL PRIMARY KEY,
                    athlete_id INTEGER NOT NULL,
                    year INTEGER,
                    games VARCHAR(255),
                    sport VARCHAR(255),
                    event VARCHAR(255),
                    result TEXT,
                    medal VARCHAR(50),
                    FOREIGN KEY (athlete_id) REFERENCES athletes (id) ON DELETE CASCADE
                );
            """)
            conn.commit()
            print("Database tables created or already exist.")
        except Exception as e:
            print(f"Error creating tables: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

def insert_athlete_data(athlete_data):
    """Inserts or updates athlete biographical and Olympic results data into the database."""
    conn = connect_db()
    if not conn:
        return

    try:
        cur = conn.cursor()
        
        # Insert/Update athlete bio data
        cur.execute(
            """
            INSERT INTO athletes (id, name, sex, born, died, height_cm, weight_kg, noc)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                sex = EXCLUDED.sex,
                born = EXCLUDED.born,
                died = EXCLUDED.died,
                height_cm = EXCLUDED.height_cm,
                weight_kg = EXCLUDED.weight_kg,
                noc = EXCLUDED.noc;
            """,
            (
                athlete_data.get('id'),
                athlete_data.get('name'),
                athlete_data.get('sex'),
                athlete_data.get('born'),
                athlete_data.get('died'),
                athlete_data.get('height_cm'),
                athlete_data.get('weight_kg'),
                athlete_data.get('noc')
            )
        )
        
        # Delete existing results for this athlete to avoid duplicates on update
        cur.execute("DELETE FROM olympic_results WHERE athlete_id = %s;", (athlete_data.get('id'),))

        # Insert Olympic results
        for result in athlete_data.get('olympic_results', []):
            cur.execute(
                """
                INSERT INTO olympic_results (athlete_id, year, games, sport, event, result, medal)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    athlete_data.get('id'),
                    result.get('year'),
                    result.get('games'),
                    result.get('sport'),
                    result.get('event'),
                    result.get('result'),
                    result.get('medal')
                )
            )
        conn.commit()
        print(f"Successfully saved data for athlete ID {athlete_data.get('id')} to database.")
    except Exception as e:
        print(f"Error saving athlete data to database: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_athlete_from_db(athlete_id):
    """Retrieves a single athlete's data and their Olympic results from the database."""
    conn = connect_db()
    if not conn:
        return None

    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        cur.execute("SELECT * FROM athletes WHERE id = %s;", (athlete_id,))
        athlete_bio = cur.fetchone()

        if athlete_bio:
            cur.execute("SELECT year, games, sport, event, result, medal FROM olympic_results WHERE athlete_id = %s ORDER BY year;", (athlete_id,))
            olympic_results = cur.fetchall()
            
            # Convert DictRow to dict for JSON serialization
            athlete_data = dict(athlete_bio)
            athlete_data['olympic_results'] = [dict(res) for res in olympic_results]
            return athlete_data
        return None
    except Exception as e:
        print(f"Error retrieving athlete data from database: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def get_all_indian_athletes_from_db():
    """Retrieves all Indian athletes and their Olympic results from the database."""
    conn = connect_db()
    if not conn:
        return []

    try:
        cur = conn.cursor(cursor_factory=DictCursor)
        cur.execute("SELECT * FROM athletes WHERE noc = 'IND';")
        indian_athletes = cur.fetchall()

        all_athletes_data = []
        for athlete_bio in indian_athletes:
            athlete_id = athlete_bio['id']
            cur.execute("SELECT year, games, sport, event, result, medal FROM olympic_results WHERE athlete_id = %s ORDER BY year;", (athlete_id,))
            olympic_results = cur.fetchall()
            
            athlete_data = dict(athlete_bio)
            athlete_data['olympic_results'] = [dict(res) for res in olympic_results]
            all_athletes_data.append(athlete_data)
        
        return all_athletes_data
    except Exception as e:
        print(f"Error retrieving all Indian athletes from database: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# --- Helper Functions for Scraping ---

def fetch_page(url):
    """Fetches the content of a given URL with a delay to be polite."""
    try:
        time.sleep(1) 
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_indian_athletes_list(html_content):
    """Parses the HTML of an Indian sport page to extract athlete names and IDs."""
    if not html_content:
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    athletes = []

    main_table = soup.find('table', class_='table') 
    if not main_table:
        return []

    for row in main_table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) > 0:
            athlete_link = cols[0].find('a')
            if athlete_link and athlete_link.text.strip():
                name = athlete_link.text.strip()
                href = athlete_link.get('href')
                athlete_id_match = re.search(r'/athletes/(\d+)', href)
                athlete_id = int(athlete_id_match.group(1)) if athlete_id_match else None
                
                if athlete_id:
                    athletes.append({
                        "name": name,
                        "id": athlete_id
                    })
    return athletes

def parse_athlete_profile(html_content, athlete_id):
    """Parses an individual athlete's profile page and saves to DB."""
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    athlete_data = {'id': athlete_id}

    name_tag = soup.find('h1')
    athlete_data['name'] = name_tag.text.strip() if name_tag else 'N/A'

    bio_div = soup.find('div', class_='athlete-bio') 
    if bio_div:
        for p_tag in bio_div.find_all('p'):
            text = p_tag.text.strip()
            if 'Born:' in text:
                athlete_data['born'] = text.replace('Born:', '').strip()
            elif 'Died:' in text:
                athlete_data['died'] = text.replace('Died:', '').strip()
            elif 'Gender:' in text:
                athlete_data['sex'] = text.replace('Gender:', '').strip()
            elif 'Height:' in text:
                height_match = re.search(r'(\d+) cm', text)
                athlete_data['height_cm'] = int(height_match.group(1)) if height_match else None # Use None for N/A in DB
            elif 'Weight:' in text:
                weight_match = re.search(r'(\d+) kg', text)
                athlete_data['weight_kg'] = int(weight_match.group(1)) if weight_match else None # Use None for N/A in DB
            elif 'NOC:' in text:
                noc_link = p_tag.find('a')
                athlete_data['noc'] = noc_link.text.strip() if noc_link else 'N/A'
                
    athlete_data['olympic_results'] = []
    results_heading = soup.find('h2', string=lambda text: text and "Olympic Games Results" in text)
    if results_heading:
        current_table = results_heading.find_next_sibling('table', class_='table')
        if current_table:
            for row in current_table.find_all('tr')[1:]:
                cols = row.find_all(['th', 'td'])
                if len(cols) >= 5:
                    year = cols[0].text.strip() if cols[0] else 'N/A'
                    games = cols[1].text.strip() if cols[1] else 'N/A'
                    sport = cols[2].text.strip() if cols[2] else 'N/A'
                    event = cols[3].text.strip() if cols[3] else 'N/A'
                    result = cols[4].text.strip() if cols[4] else 'N/A'
                    
                    medal = None
                    if 'Gold' in result:
                        medal = 'Gold'
                    elif 'Silver' in result:
                        medal = 'Silver'
                    elif 'Bronze' in result:
                        medal = 'Bronze'

                    athlete_data['olympic_results'].append({
                        "year": int(year) if year.isdigit() else None, # Convert year to int
                        "games": games,
                        "sport": sport,
                        "event": event,
                        "result": result,
                        "medal": medal
                    })
    
    # Save the scraped data to the database
    insert_athlete_data(athlete_data)
    return athlete_data

# --- API Endpoints ---

@app.route('/api/indian_athletes_list/<string:sport_code>', methods=['GET'])
def get_indian_athletes_by_sport(sport_code):
    """
    API endpoint to list Indian athletes for a given sport code by scraping Olympedia.
    This endpoint does NOT store the full athlete profile data, only lists.
    Example: /api/indian_athletes_list/ATH (for Athletics)
    """
    url = f"{BASE_URL}/countries/IND/sports/{sport_code.upper()}.1"
    
    html_content = fetch_page(url)
    if not html_content:
        return jsonify({"error": f"Could not retrieve data for sport code {sport_code}. Check if the sport code is valid or if the website structure has changed."}), 500

    athletes = parse_indian_athletes_list(html_content)
    if not athletes:
        return jsonify({"message": f"No athletes found for sport code {sport_code} or parsing failed."}), 404

    return jsonify(athletes)

@app.route('/api/athlete_history/<int:athlete_id>', methods=['GET'])
def get_athlete_historical_data(athlete_id):
    """
    API endpoint to retrieve detailed historical data for a specific athlete.
    First tries to retrieve from DB. If not found, scrapes Olympedia, stores in DB, then returns.
    Example: /api/athlete_history/148612 (Neeraj Chopra)
    """
    # 1. Try to get data from the database first (caching)
    athlete_data = get_athlete_from_db(athlete_id)
    if athlete_data:
        print(f"Retrieved athlete ID {athlete_id} from database.")
        return jsonify(athlete_data)

    # 2. If not in DB, scrape Olympedia
    print(f"Athlete ID {athlete_id} not found in DB. Scraping Olympedia...")
    url = f"{BASE_URL}/athletes/{athlete_id}"
    html_content = fetch_page(url)
    if not html_content:
        return jsonify({"error": f"Could not retrieve data for athlete ID {athlete_id} from Olympedia. Check if the ID is valid or if the website structure has changed."}), 500

    # parse_athlete_profile also handles saving to DB
    athlete_data = parse_athlete_profile(html_content, athlete_id)
    if not athlete_data:
        return jsonify({"message": f"No data found for athlete ID {athlete_id} or parsing failed."}), 404

    return jsonify(athlete_data)

@app.route('/api/all_indian_olympians', methods=['GET'])
def get_all_indian_olympians():
    """
    API endpoint to retrieve all historical data for Indian athletes
    currently stored in the PostgreSQL database.
    This endpoint does NOT trigger scraping. Data must be populated via
    /api/athlete_history/<athlete_id> calls or a separate script.
    """
    all_athletes = get_all_indian_athletes_from_db()
    if not all_athletes:
        return jsonify({"message": "No Indian athlete data found in the database. Use /api/athlete_history/<athlete_id> to populate data first."}), 404
    return jsonify(all_athletes)

# --- Initial Setup on App Start ---
# Ensure tables are created when the Flask app starts
with app.app_context():
    create_tables()

if __name__ == '__main__':
    app.run(debug=True)
