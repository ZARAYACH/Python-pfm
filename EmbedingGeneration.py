from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import pymysql
from sqlalchemy import create_engine, text

# DB config
DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = 'localhost'
DB_NAME = 'arxiv_db'
DB_PORT = 3306

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=False)

model = SentenceTransformer('all-MiniLM-L6-v2')

with engine.connect() as conn:
    results = conn.execute(text("SELECT id, abstract FROM articles WHERE abstract IS NOT NULL")).fetchall()

ids = []
abstracts = []

for r in results:
    ids.append(r[0])
    abstracts.append(r[1])

embeddings = model.encode(abstracts, convert_to_numpy=True)

index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(embeddings)

np.save("article_ids.npy", np.array(ids))
faiss.write_index(index, "arxiv_faiss.index")

def search_articles(query, top_k=5):
    model = SentenceTransformer('all-MiniLM-L6-v2')
    query_embedding = model.encode([query], convert_to_numpy=True)

    index = faiss.read_index("arxiv_faiss.index")
    article_ids = np.load("article_ids.npy")

    D, I = index.search(query_embedding, top_k)
    closest_ids = article_ids[I[0]].tolist()

    with engine.connect() as conn:
        placeholders = ",".join([f":id{i}" for i in range(len(closest_ids))])
        sql = f"SELECT title, abstract, published FROM articles WHERE id IN ({placeholders})"
        params = {f"id{i}": int(closest_ids[i]) for i in range(len(closest_ids))}
        rows = conn.execute(text(sql), params).fetchall()

    return rows


l
results = search_articles("4K resolution")
for title, abstract, published in results:
    print(f"📄 {title} ({published})\n🧠 {abstract[:300]}...\n")
