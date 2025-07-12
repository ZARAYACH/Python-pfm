import requests
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

API_KEY = '02982532183b6c794f3bc458e2e1a567'
QUERY = 'TITLE(computer science)'
MAX_RESULTS = 100
RESULTS_PER_PAGE = 25
SLEEP_TIME = 1

DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'scopus_db'

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    echo=False
)
def create_table():
    with engine.connect() as conn:
         conn.execute(text('''
                    CREATE TABLE IF NOT EXISTS articles (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title TEXT,
                        abstract TEXT,
                        authors TEXT,
                        publication_date VARCHAR(255),
                        publication_name TEXT,
                        doi VARCHAR(255),
                        keywords TEXT,
                        scopus_id VARCHAR(255)
                    )
                '''))

def safe_get(entry, field, default=""):
    return entry.get(field, default)

def parse_authors(entry):
    if 'author' in entry:
        return "; ".join([a.get('authname', '') for a in entry['author']])
    return ""

def extract_and_store(entries):
#     print(entries)
    articles = []
    for entry in entries:
        articles.append({
            "title": safe_get(entry, "dc:title"),
            "abstract": safe_get(entry, "dc:description"),
            "authors": parse_authors(entry),
            "publication_date": safe_get(entry, "prism:coverDate"),
            "publication_name": safe_get(entry, "prism:publicationName"),
            "doi": safe_get(entry, "prism:doi"),
            "keywords": safe_get(entry, "authkeywords"),
            "scopus_id": safe_get(entry, "dc:identifier")
        })

    df = pd.DataFrame(articles)
    df.to_sql('articles', engine, if_exists='append', index=False)

def fetch_articles():
    start = 0
    headers = {
        "X-ELS-APIKey": API_KEY,
        "Accept": "application/json"
    }

    while start < MAX_RESULTS:
        print(f"Fetching records {start} to {start + RESULTS_PER_PAGE}...")
        url = (
            f"https://api.elsevier.com/content/search/scopus"
            f"?query={QUERY}&count={RESULTS_PER_PAGE}&start={start}"
        )

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            break

        data = response.json()
        print(data)
        entries = data.get("search-results", {}).get("entry", [])
        if not entries:
            print("No more entries.")
            break

        extract_and_store(entries)
        start += RESULTS_PER_PAGE
        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    create_table()
    fetch_articles()
    print("Done. Articles saved in 'scopus_articles.db'")
