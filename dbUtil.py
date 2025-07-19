from sqlalchemy import text, create_engine

DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'arxiv_db_v2'

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
                              categories TEXT,
                              doi VARCHAR(255)
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

def insert_article(conn, article):
    conn.execute(text('''
                      INSERT  INTO articles (title, abstract, published, arxiv_id, categories, doi)
        VALUES (:title, :abstract, :published, :arxiv_id, :categories, :doi)
                      '''), article)
    return conn.execute(text("SELECT id FROM articles WHERE arxiv_id = :aid"), {"aid": article['arxiv_id']}).fetchone()[0]

def insert_author(conn, name):
    conn.execute(text("INSERT IGNORE INTO authors (name) VALUES (:name)"), {"name": name})
    return conn.execute(text("SELECT id FROM authors WHERE name = :name"), {"name": name}).fetchone()[0]

def link_article_author(conn, article_id, author_id):
    conn.execute(text('''
                      INSERT ignore INTO article_authors (article_id, author_id)
        VALUES (:article_id, :author_id)
                      '''), {"article_id": article_id, "author_id": author_id})


def insert_articles_bulk(conn, articles):
    # Prepare values
    insert_stmt = text('''
                       INSERT INTO articles (title, abstract, published, arxiv_id, categories, doi)
                       VALUES (:title, :abstract, :published, :arxiv_id, :categories, :doi)
                           ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)
                       ''')

    conn.execute(insert_stmt, articles)

    arxiv_ids = [a["arxiv_id"] for a in articles]
    id_rows = conn.execute(
    text("SELECT id, arxiv_id FROM articles WHERE arxiv_id IN :arxiv_ids"),
    {"arxiv_ids": tuple(arxiv_ids)}).mappings().all()
    return {row["arxiv_id"]: row["id"] for row in id_rows}


def insert_authors_bulk(conn, names):
    names = list(set(names))  # Deduplicate
    conn.execute(
        text("INSERT IGNORE INTO authors (name) VALUES (:name)"),
        [{"name": name} for name in names]
    )

    rows = conn.execute(
        text("SELECT id, name FROM authors WHERE name IN :names"),
        {"names": tuple(names)}
    ).mappings().all()

    return {row["name"]: row["id"] for row in rows}

def link_article_authors_bulk(conn, links):
    conn.execute(
        text("""
             INSERT IGNORE INTO article_authors (article_id, author_id)
            VALUES (:article_id, :author_id)
             """),
        links
    )
