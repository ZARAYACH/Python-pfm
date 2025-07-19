import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from cleanUpScript import clean_text, normalize_author
from dbUtil import insert_article, insert_author, link_article_author, engine
conn =  engine.connect()


def process_article(article_data):
    print('process started. for ', article_data)
    try:
        article = {
            "title": normalize_author(article_data.get("title", "")),
            "abstract": clean_text(article_data.get("abstract", "").strip()),
            "published": None,
            "arxiv_id": article_data["id"],
            "categories": article_data.get("categories", ""),
            "doi": article_data.get("doi", None)
        }

        if article_data.get("versions"):
            created = article_data["versions"][0].get("created", "")
            try:
                article["published"] = datetime.strptime(created, "%a, %d %b %Y %H:%M:%S %Z").date()
            except:
                pass
        article_id = insert_article(conn, article)
        print("article inserted. id :" + str(article_id))
        authors_parsed = article_data.get("authors_parsed", [])
        for author in authors_parsed:
            name_parts = []
            for part in author:
                try:
                    clean_part = str(part).strip()
                    if clean_part:
                        name_parts.append(clean_part)
                except Exception as e:
                      print(f"Skipping invalid name part {part}: {e}")
                      continue

                name = " ".join(name_parts)
                if name:
                    author_id = insert_author(conn, name)
                    link_article_author(conn, article_id, author_id)
        conn.commit()
    except Exception as e:
                print(f"Error processing article {article_data.get('id', 'unknown')}: {e}")

def insert_articles_from_file(file_path, max_workers=1, chunk_size=100):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        chunk = []

        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    article_data = json.loads(line)
                    chunk.append(article_data)
                except Exception as e:
                    print(f"Error parsing line: {e}")
                    continue

                if len(chunk) >= chunk_size:
                    for article in chunk:
                        futures.append(executor.submit(process_article, article))
                    chunk = []

            for article in chunk:
                futures.append(executor.submit(process_article, article))

        for future in futures:
            future.result()
