import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import faiss
from sklearn.manifold import TSNE
from sentence_transformers import SentenceTransformer

from dbUtil import engine

# Set up environment
conn = engine.connect()
visualisations_path = 'out/visualisations/'
os.makedirs(visualisations_path, exist_ok=True)

sns.set(style='whitegrid')

def plot_articles_per_year():
    query = """
            SELECT YEAR(published) AS year, COUNT(*) AS count
            FROM articles
            WHERE published IS NOT NULL
            GROUP BY year
            ORDER BY year \
            """
    df = pd.read_sql(query, conn)
    plt.figure(figsize=(10,6))
    sns.barplot(x='year', y='count', data=df, palette='crest')
    plt.title("Articles Published per Year")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(visualisations_path, "articles_per_year.png"))
    plt.show()

def plot_top_authors():
    query = """
            SELECT a.name, COUNT(*) AS count
            FROM authors a
                JOIN article_authors aa ON a.id = aa.author_id
            GROUP BY a.name
            ORDER BY count DESC \
            """
    df = pd.read_sql(query, conn)
    plt.figure(figsize=(10,6))
    sns.barplot(x='count', y='name', data=df, palette='viridis')
    plt.title("Top 10 Most Prolific Authors")
    plt.xlabel("Number of Articles")
    plt.ylabel("Author")
    plt.tight_layout()
    plt.savefig(os.path.join(visualisations_path, "top_authors.png"))
    plt.show()

def plot_category_distribution():
    query = """
            SELECT categories
            FROM articles
            WHERE categories IS NOT NULL \
            """
    df = pd.read_sql(query, conn)
    all_cats = []
    for row in df['categories']:
        all_cats.extend(row.strip().split())
    cat_df = pd.DataFrame({'category': all_cats})
    top_cats = cat_df['category'].value_counts().nlargest(10).reset_index()
    top_cats.columns = ['category', 'count']

    plt.figure(figsize=(10, 6))
    sns.barplot(x='count', y='category', data=top_cats, palette='rocket')
    plt.title("Top 10 Categories")
    plt.xlabel("Number of Articles")
    plt.ylabel("Category")
    plt.tight_layout()
    plt.savefig(os.path.join(visualisations_path, "categories.png"))
    plt.show()

def plot_doi_coverage():
    query = """
            SELECT
                SUM(CASE WHEN doi IS NOT NULL AND doi != '' THEN 1 ELSE 0 END) AS with_doi,
                COUNT(*) AS total
            FROM articles \
            """
    df = pd.read_sql(query, conn)
    with_doi = df['with_doi'].values[0]
    total = df['total'].values[0]
    without_doi = total - with_doi
    plt.figure(figsize=(6,6))
    plt.pie([with_doi, without_doi], labels=['With DOI', 'Without DOI'], autopct='%1.1f%%', colors=['#66b3ff','#ff9999'])
    plt.title("DOI Coverage")
    plt.tight_layout()
    plt.savefig(os.path.join(visualisations_path, "doi_coverage.png"))
    plt.show()

def faiss_visualization():
    try:
        print("Loading FAISS index and article IDs...")
        index = faiss.read_index("out/arxiv_faiss.index")
        article_ids = np.load("out/article_ids.npy")

        print("Fetching articles with categories...")
        format_ids = ','.join(map(str, article_ids.tolist()))
        query = f"""
            SELECT id, title, abstract, categories
            FROM articles
            WHERE id IN ({format_ids})
        """
        df = pd.read_sql(query, conn)
        df = df.set_index('id').loc[article_ids]  # ensure FAISS-aligned order

        texts = (df['title'].fillna('') + ". " + df['abstract'].fillna('')).tolist()

        print("Embedding articles...")
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embeddings = model.encode(texts, show_progress_bar=True)

        print("Reducing dimensions with TSNE...")
        reduced = TSNE(n_components=2, random_state=42).fit_transform(embeddings)

        df['main_category'] = df['categories'].fillna('').apply(lambda x: x.split()[0] if x else 'unknown')

        print("Plotting category-colored TSNE...")
        plt.figure(figsize=(12, 8))
        sns.scatterplot(
            x=reduced[:, 0],
            y=reduced[:, 1],
            hue=df['main_category'],
            palette='tab20',
            legend='full',
            alpha=0.7
        )
        plt.title("TSNE Projection of Article Embeddings by Main Category")
        plt.xlabel("TSNE-1")
        plt.ylabel("TSNE-2")
        plt.tight_layout()
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title='Category')
        plt.savefig(os.path.join(visualisations_path,"faiss_tsne_by_category.png"), bbox_inches='tight')
        plt.show()
    except Exception as e:
        print("Error during FAISS visualization:", e)

def generate_visualisations() :
    plot_articles_per_year()
    plot_top_authors()
    plot_category_distribution()
    plot_doi_coverage()
    faiss_visualization()
