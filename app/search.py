import faiss
import numpy as np
from sqlalchemy import text

from EmbedingGeneration import model
from dbUtil import engine

index = faiss.read_index("../out/arxiv_faiss.index")
article_ids = np.load("../out/article_ids.npy")


def search_articles(query, top_k=5):
    query_embedding = model.encode([query], convert_to_numpy=True)

    D, I = index.search(query_embedding, top_k)
    closest_ids = article_ids[I[0]].tolist()

    with engine.connect() as conn:
        placeholders = ",".join([f":id{i}" for i in range(len(closest_ids))])
        sql = f"SELECT id, title, abstract, published, doi FROM articles WHERE id IN ({placeholders})"
        params = {f"id{i}": int(closest_ids[i]) for i in range(len(closest_ids))}
        rows = conn.execute(text(sql), params).mappings().all()
    return rows