import gc
import json
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from cleanUpScript import clean_text, normalize_author
from dbUtil import insert_article, insert_author, link_article_author, engine, link_article_authors_bulk, \
    insert_articles_bulk, insert_authors_bulk

data_queue = queue.Queue(maxsize=100)

def process_chunk(articles_chunk):
    thread_id = threading.get_ident()
    print(f"[Reader {thread_id}]  Starting processing of chunk ({len(articles_chunk)} articles)")

    prepared_articles = []
    article_author_links = []
    all_author_names = []

    for article_data in articles_chunk:
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
                    article["published"] = datetime.strptime(
                        created, "%a, %d %b %Y %H:%M:%S %Z"
                    ).date()
                except Exception as e:
                    print(f"⚠️ Could not parse date for {article['arxiv_id']}: {e}")

            prepared_articles.append(article)
            authors_parsed = article_data.get("authors_parsed", [])
            for author in authors_parsed:
                name_parts = [str(part).strip() for part in author if part and str(part).strip()]
                name = " ".join(name_parts)
                if name:
                    all_author_names.append(name)

        except Exception as e:
            print(f"Error processing article {article_data.get('id', 'unknown')}: {e}")

    try:
        # with engine.connect() as conn :
        #     # Insert articles and authors
        #     arxiv_id_to_article_id = insert_articles_bulk(conn, prepared_articles)
        #     conn.commit()
        #     name_to_author_id = insert_authors_bulk(conn, all_author_names)
        #     conn.commit()
        #     print("✅ Articles inserted:", arxiv_id_to_article_id)
        #     print("✅ Authors inserted:", name_to_author_id)
        #
        #     # Link articles and authors
        #     for article in prepared_articles:
        #         article_id = arxiv_id_to_article_id.get(article["arxiv_id"])
        #         for article_data in articles_chunk:
        #             if article_data["id"] == article["arxiv_id"]:
        #                 authors_parsed = article_data.get("authors_parsed", [])
        #                 for author in authors_parsed:
        #                     name = " ".join(str(p).strip() for p in author if str(p).strip())
        #                     author_id = name_to_author_id.get(name)
        #                     if article_id and author_id:
        #                         article_author_links.append({
        #                             "article_id": article_id,
        #                             "author_id": author_id
        #                         })
        #
        #     print("🧩 Linking article-author relations")
        #     link_article_authors_bulk(conn, article_author_links)
        #     conn.commit()
        #     print(f"✅ Finished chunk of {len(prepared_articles)} articles")
        print(f"[Reader {thread_id}] Putting data into queue")
        data_queue.put((prepared_articles, all_author_names, articles_chunk))
    except Exception as e:
        print(f"🔥 Error during DB operations: {e}")
        print(f"[Reader {thread_id}] ❌ Exception: {e}")


def insert_articles_from_file(file_path, max_workers=20, chunk_size=1000):
    futures = []
    chunk = []

    writer_thread = threading.Thread(target=writer_thread_fn)
    writer_thread.start()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    article_data = json.loads(line)
                    chunk.append(article_data)
                except Exception as e:
                    print(f"⚠️ Error parsing line: {e}")
                    continue

                if len(chunk) >= chunk_size:
                    futures.append(executor.submit(process_chunk, chunk.copy()))
                    chunk.clear()

            if chunk:
                futures.append(executor.submit(process_chunk, chunk.copy()))

        for future in futures:
            future.result()

    data_queue.put(None)
    writer_thread.join()
    print("All articles processed.")


def writer_thread_fn():
    thread_id = threading.get_ident()


    while True:
        try:
            item = data_queue.get(timeout=5)
        except queue.Empty:
            break  # Stop when queue is empty and timeout reached

        if item is None:
            break  # Special signal to terminate

        prepared_articles, all_author_names, articles_chunk = item
        article_author_links = []

        try:
            with engine.connect() as conn:
                arxiv_id_to_article_id = insert_articles_bulk(conn, prepared_articles)
                conn.commit()
                print(f"[Writer {thread_id}] Articles inserted: {len(arxiv_id_to_article_id)}")

                name_to_author_id = insert_authors_bulk(conn, all_author_names)
                conn.commit()
                print(f"[Writer {thread_id}] Authors inserted: {len(name_to_author_id)}")

                for article in prepared_articles:
                    article_id = arxiv_id_to_article_id.get(article["arxiv_id"])
                    for article_data in articles_chunk:
                        if article_data["id"] == article["arxiv_id"]:
                            authors_parsed = article_data.get("authors_parsed", [])
                            for author in authors_parsed:
                                name = " ".join(str(p).strip() for p in author if str(p).strip())
                                author_id = name_to_author_id.get(name)
                                if article_id and author_id:
                                    article_author_links.append({
                                        "article_id": article_id,
                                        "author_id": author_id
                                    })

                link_article_authors_bulk(conn, article_author_links)
                print(f"[Writer {thread_id}]  Linked {len(article_author_links)} relations")
                conn.commit()
                article_author_links.clear()
                del article_author_links, prepared_articles, all_author_names, articles_chunk
                gc.collect()
        except Exception as e:
            print(f"Writer thread DB error: {e}")
