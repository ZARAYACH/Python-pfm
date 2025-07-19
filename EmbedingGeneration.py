from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
from cleanUpScript import clean_text
from dbUtil import engine

model = SentenceTransformer('all-MiniLM-L6-v2')
BATCH_SIZE = 1000
MAX_THREADS = 1

def process_embeddings():
    with engine.connect() as conn:
        total = conn.execute(text("""
                                  SELECT COUNT(*) FROM articles
                                  WHERE abstract IS NOT NULL AND abstract != ''
                                  """)).scalar()
        print(f"Total valid articles with abstracts: {total}")

    offsets = list(range(0, total, BATCH_SIZE))

    all_ids = []
    all_embeddings = []

    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            print(f"Submitting {len(offsets)} batches using {MAX_THREADS} threads...")
            futures = [executor.submit(fetch_page, offset) for offset in offsets]
            for future in futures:
                result = future.result()
                if result:
                    ids, embeddings = result
                    if embeddings is not None:
                        all_ids.extend(ids)
                        all_embeddings.append(embeddings)
    except Exception as e:
        print(f"Error in batch thread: {e}")

    if all_embeddings:
        all_embeddings_np = np.vstack(all_embeddings)
        print(f"Combined total embeddings shape: {all_embeddings_np.shape}")

        index = faiss.IndexIDMap(faiss.IndexFlatL2(all_embeddings_np.shape[1]))
        index.add_with_ids(all_embeddings_np, np.array(all_ids))

        # Save index and IDs
        faiss.write_index(index, "out/arxiv_faiss.index")
        np.save("out/article_ids.npy", np.array(all_ids))
        print("💾 FAISS index saved to out/arxiv_faiss.index")
        print("💾 Article IDs saved to out/article_ids.npy")
    else:
        print("⚠️ No embeddings to index — check data and embedding function.")

    print("✅ FAISS index created and saved.")

def fetch_page(offset):
    print(f"Fetching batch at offset {offset}")
    with engine.connect() as conn:
        query = text(f"""
            SELECT id, abstract FROM articles
            WHERE abstract IS NOT NULL AND abstract != ''
            LIMIT :limit OFFSET :offset
        """)
        results = conn.execute(query, {"limit": BATCH_SIZE, "offset": offset}).fetchall()
        if results:
            ids = [r[0] for r in results]
            abstracts = [clean_text(r[1]) for r in results]
            embeddings = model.encode(abstracts, convert_to_numpy=True, show_progress_bar=False)
            return ids, embeddings
        return [], None

