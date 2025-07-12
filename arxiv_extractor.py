import feedparser
import time
from sqlalchemy import create_engine, text

DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'arxiv_db'

ARXIV_CATEGORY = 'math'
MAX_RESULTS = 1000
RESULTS_PER_CALL = 25
BASE_URL = "http://export.arxiv.org/api/query?"

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=False)

def create_tables():
    with engine.connect() as conn:
        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS articles (
                                                                  id INT AUTO_INCREMENT PRIMARY KEY,
                                                                  title TEXT,
                                                                  abstract TEXT,
                                                                  published DATE,
                                                                  arxiv_id VARCHAR(255) UNIQUE,
                              categories TEXT
                              )
                          '''))

        conn.execute(text('''
                          CREATE TABLE IF NOT EXISTS authors (
                                                                 id INT AUTO_INCREMENT PRIMARY KEY,
                                                                 name TEXT,
                                                                 UNIQUE(name)
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

# === DB INSERTS ===
def insert_article(conn, article):
    conn.execute(text('''
                      INSERT IGNORE INTO articles (title, abstract, published, arxiv_id, categories)
        VALUES (:title, :abstract, :published, :arxiv_id, :categories)
                      '''), article)
    return conn.execute(text("SELECT id FROM articles WHERE arxiv_id = :aid"), {"aid": article['arxiv_id']}).fetchone()[0]

def insert_author(conn, name):
    conn.execute(text("INSERT IGNORE INTO authors (name) VALUES (:name)"), {"name": name})
    return conn.execute(text("SELECT id FROM authors WHERE name = :name"), {"name": name}).fetchone()[0]

def link_article_author(conn, article_id, author_id):
    conn.execute(text('''
                      INSERT IGNORE INTO article_authors (article_id, author_id)
        VALUES (:article_id, :author_id)
                      '''), {"article_id": article_id, "author_id": author_id})

def fetch_and_store_arxiv():
    for start in range(0, MAX_RESULTS, RESULTS_PER_CALL):
        print(f"📥 Fetching {start} to {start + RESULTS_PER_CALL}")
        query = (
            f"search_query=all:computer"
            f"&start={start}&max_results={RESULTS_PER_CALL}&sortBy=submittedDate&sortOrder=descending"
        )
        feed = feedparser.parse(BASE_URL + query)

        if not feed.entries:
            print("No more results.")
            break

        with engine.begin() as conn:
            for entry in feed.entries:
                arxiv_id = entry.id.split('/abs/')[-1]
                article = {
                    "title": entry.title,
                    "abstract": entry.summary,
                    "published": entry.published.split('T')[0],
                    "arxiv_id": arxiv_id,
                    "categories": ", ".join(entry.tags[i]['term'] for i in range(len(entry.tags))) if 'tags' in entry else ""
                }

                article_id = insert_article(conn, article)

                for author in entry.authors:
                    name = author.name
                    author_id = insert_author(conn, name)
                    link_article_author(conn, article_id, author_id)

        time.sleep(1)

# === MAIN ===
if __name__ == "__main__":
    create_tables()
    fetch_and_store_arxiv()
    print("✅ Done: ArXiv articles and authors stored.")
