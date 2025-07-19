from flask import Flask, render_template, request, jsonify
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

from app.search import search_articles

tokenizer = AutoTokenizer.from_pretrained("microsoft/DialoGPT-medium")
model = AutoModelForCausalLM.from_pretrained("microsoft/DialoGPT-medium")

app = Flask(__name__,template_folder='templates')

@app.route("/")
def index():
    return render_template('chat.html')

@app.route("/get", methods=["GET", "POST"])
def chat():
    msg = request.form["msg"]
    print(f"[DEBUG] Received input: {msg}")
    try:
        articles = []

        print("[DEBUG] Detected research query.")
        articles = search_articles(msg)
        print(f"[DEBUG] Results returned from DB: {articles}")

        filtered_articles = [
            {"title": r["title"],
             "authors": r.get("authors", "Unknown"),
             "abstract": r.get("abstract",""),
             "published":r.get("published", ""),
             "doi": r.get("doi","")}
            for r in articles[:5]
        ]

        return jsonify({
            "articles": filtered_articles
        })

    except Exception as e:
        print(f"[ERROR]: {e}")
        return jsonify({
            "articles": []
        })

def is_research_query(text: str) -> bool:
    keywords = ["paper", "research", "article", "abstract", "study", "author", "publication", "findings"]
    return any(k in text.lower() for k in keywords)


if __name__ == '__main__':
    app.run(debug=True)
