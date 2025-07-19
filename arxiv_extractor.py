import feedparser
import time
from EmbedingGeneration import process_embeddings
from dbUtil import create_tables, insert_article, insert_author, link_article_author, engine
from importfromfile_v2 import insert_articles_from_file
from visulisations import generate_visualisations

ARXIV_CATEGORY = 'math'
MAX_RESULTS = 1000
RESULTS_PER_CALL = 25
BASE_URL = "http://export.arxiv.org/api/query?"


def fetch_and_store_arxiv():
    for start in range(0, MAX_RESULTS, RESULTS_PER_CALL):
        print(f"Fetching {start} to {start + RESULTS_PER_CALL}")
        query = (
            f"search_query=category:cs"
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
                    "doi": "",
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


if __name__ == "__main__":
    create_tables()
    fetch_and_store_arxiv()
    insert_articles_from_file("./data/arxiv-metadata-oai-snapshot.json")
    process_embeddings()
    generate_visualisations()
    print("✅ Done")
