import requests
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

API_KEY = '02982532183b6c794f3bc458e2e1a567'
QUERY = ('TITLE(computer science)')
MAX_RESULTS = 1000
RESULTS_PER_PAGE = 25
SLEEP_TIME = 1

DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'scopus_db'

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=False)

def create_tables():
    with engine.connect() as conn:
        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS articles (
                                                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                                                  title TEXT,
                                                                  abstract TEXT,
                                                                  cover_date VARCHAR(255),
                              publication_name TEXT,
                              doi VARCHAR(255),
                              scopus_id VARCHAR(255) UNIQUE,
                              keywords TEXT,
                              subject_areas TEXT
                              )
                          '''))
        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS authors (
                                                                 id INT AUTO_INCREMENT PRIMARY KEY,
                                                                 scopus_author_id VARCHAR(255),
                              orcid VARCHAR(255),
                              preferred_name TEXT,
                              UNIQUE(scopus_author_id)
                              )
                          '''))
        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS affiliations (
                                                                      id INT AUTO_INCREMENT PRIMARY KEY,
                                                                      institution_name TEXT,
                                                                      city TEXT,
                                                                      country TEXT
                              )
                          '''))
        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS article_authors (
                                                                         article_id INT,
                                                                         author_id INT,
                                                                         PRIMARY KEY(article_id, author_id),
                              FOREIGN KEY(article_id) REFERENCES articles(id),
                              FOREIGN KEY(author_id) REFERENCES authors(id)
                              )
                          '''))
        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS author_affiliations (
                                                                             author_id INT,
                                                                             affiliation_id INT,
                                                                             PRIMARY KEY(author_id, affiliation_id),
                              FOREIGN KEY(author_id) REFERENCES authors(id),
                              FOREIGN KEY(affiliation_id) REFERENCES affiliations(id)
                              )
                          '''))

def insert_article(conn, article):
    conn.execute(text('''
                      INSERT IGNORE INTO articles
        (title, abstract, cover_date, publication_name, doi, scopus_id, keywords, subject_areas)
        VALUES (:title, :abstract, :cover_date, :publication_name, :doi, :scopus_id, :keywords, :subject_areas)
                      '''), article)
    return conn.execute(text("SELECT id FROM articles WHERE scopus_id = :sid"), {"sid": article['scopus_id']}).fetchone()[0]

def insert_author(conn, author):
    conn.execute(text('''
                      INSERT IGNORE INTO authors (scopus_author_id, orcid, preferred_name)
        VALUES (:scopus_author_id, :orcid, :preferred_name)
                      '''), author)
    return conn.execute(text("SELECT id FROM authors WHERE scopus_author_id = :sid"), {"sid": author['scopus_author_id']}).fetchone()[0]

def insert_affiliation(conn, aff):
    conn.execute(text('''
                      INSERT IGNORE INTO affiliations (institution_name, city ,country)
        VALUES (:institution_name, :city ,:country)
                      '''), aff)

    return conn.execute(text("SELECT id FROM affiliations WHERE institution_name = :institution_name && city =:city && country= :country "), aff).fetchone()[0]

def link_article_author(conn, article_id, author_id):
    conn.execute(text('''
                      INSERT IGNORE INTO article_authors (article_id, author_id)
        VALUES (:article_id, :author_id)
                      '''), {"article_id": article_id, "author_id": author_id})

def link_author_affiliation(conn, author_id, affiliation_id):
    conn.execute(text('''
                      INSERT IGNORE INTO author_affiliations (author_id, affiliation_id)
        VALUES (:author_id, :affiliation_id)
                      '''), {"author_id": author_id, "affiliation_id": affiliation_id})

def process_full_article(data):
    abstract = data.get("abstracts-retrieval-response", {})
    coredata = abstract.get("coredata", {})
    authors_list = coredata.get("dc:creator", {}).get("author", [])
    affiliations_list = abstract.get("affiliation", [])
    eid = coredata.get("pii", "")
    if eid == "" : return
    fetched_article = fetch_article(eid)

    if fetched_article is None:
        return

    article = {
        "title": fetched_article.get("dc:title", ""),
        "abstract": fetched_article.get("dc:description", ""),
        "cover_date": fetched_article.get("prism:coverDate", ""),
        "publication_name": fetched_article.get("prism:publicationName", ""),
        "doi": coredata.get("prism:doi", ""),
        "scopus_id": coredata.get("dc:identifier", ""),
        "keywords": coredata.get("authkeywords", ""),
        "subject_areas": ""
    }

    if not authors_list:
        print(f"⏩ Skipping article with no authors: {article['title']}")
        return

    with engine.begin() as conn:
        article_id = insert_article(conn, article)

        for author in authors_list:
            au_id = author.get("@auid", "")
            name = author.get("ce:indexed-name", "")
            orcid = author.get("orcid", "")

            author_data = {
                "scopus_author_id": au_id,
                "orcid": orcid,
                "preferred_name": name
            }

            author_id = insert_author(conn, author_data)
            link_article_author(conn, article_id, author_id)
            if isinstance(affiliations_list, dict) :
                aff_data = {
                    "institution_name": affiliations_list.get("affilname", ""),
                    "country": affiliations_list.get("affiliation-country", ""),
                    "city": affiliations_list.get("affiliation-city","")
                }
                db_aff_id = insert_affiliation(conn, aff_data)
                link_author_affiliation(conn, author_id, db_aff_id)
            else:
                for aff in affiliations_list:
                    aff_data = {
                        "institution_name": aff.get("affilname", ""),
                        "country": aff.get("affiliation-country", ""),
                        "city": aff.get("affiliation-city","")
                    }
                    db_aff_id = insert_affiliation(conn, aff_data)
                    link_author_affiliation(conn, author_id, db_aff_id)

def fetch_articles():
    headers = {
        "X-ELS-APIKey": API_KEY,
        "Accept": "application/json"
    }

    start = 0
    while start < MAX_RESULTS:
        print(f"🔍 Searching articles {start} to {start + RESULTS_PER_PAGE}")
        search_url = f"https://api.elsevier.com/content/search/scopus?query={QUERY}&count={RESULTS_PER_PAGE}&start={start}"
        res = requests.get(search_url, headers=headers)
        if res.status_code != 200:
            print("❌ Search API Error:", res.status_code, res.text)
            break

        data = res.json()
        entries = data.get("search-results", {}).get("entry", [])
        if not entries:
            break

        eids = [entry.get("eid") for entry in entries if "eid" in entry]

        for eid in eids:
            abs_url = f"https://api.elsevier.com/content/abstract/eid/{eid}"
            abs_res = requests.get(abs_url, headers=headers)
            if abs_res.status_code != 200:
                print(f"Error fetching abstract for {eid}")
                continue

            process_full_article(abs_res.json())
            time.sleep(0.5)

        start += RESULTS_PER_PAGE
        time.sleep(SLEEP_TIME)

def fetch_article(eid) :
    headers = {
        "X-ELS-APIKey": API_KEY,
        "Accept": "application/json"
    }
    search_url = f"https://api.elsevier.com/content/article/pii/{eid}"
    res = requests.get(search_url, headers=headers)
    if res.status_code != 200:
        print("❌ Search API Error:", res.status_code, res.text)
        return None
    data = res.json()
    return data.get("full-text-retrieval-response", {}).get("coredata", {})

if __name__ == "__main__":
    create_tables()
    fetch_articles()
    print("✅ Done! Articles, authors, and affiliations saved.")
