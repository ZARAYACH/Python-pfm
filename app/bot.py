from flask import Flask, render_template, request, jsonify
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

from app.search import search_articles

tokenizer = AutoTokenizer.from_pretrained("microsoft/DialoGPT-medium")
model = AutoModelForCausalLM.from_pretrained("microsoft/DialoGPT-medium")
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token
app = Flask(__name__,template_folder='templates')

@app.route("/")
def index():
    return render_template('chat.html')


@app.route("/get", methods=["GET", "POST"])
def chat():
    msg = request.form["msg"]
    return get_chat_response(msg)

def get_chat_response(text):
    print(f"[DEBUG] Received input: {text}")

    try:
        if is_research_query(text):
            print("[DEBUG] Detected research query.")
            results = search_articles(text)
            print(f"[DEBUG] Results returned from DB: {results}")

            if results:
                try:
                    context = "\n".join([
                        f"Title: {r['title']}\nAbstract: {r['abstract']}" for r in results[:3]
                    ])
                except Exception as e:
                    print(f"[ERROR] Failed to build context from results: {e}")
                    context = ""

                prompt = (
                    f"The user asked: '{text}'\n"
                    f"Based on the following articles:\n{context}\n"
                    "Answer the user's question:"
                )
            else:
                print("[DEBUG] No articles found.")
                prompt = f"No relevant research articles were found. Still, here's an answer to: '{text}'"
        else:
            print("[DEBUG] Detected general query.")
            prompt = text

        print(f"[DEBUG] Final prompt:\n{prompt}")

        encoded_input = tokenizer.encode_plus(
            prompt,
            return_tensors='pt',
            add_special_tokens=True,
            return_attention_mask=True,
            padding=True,
            truncation=True
        )

        input_ids = encoded_input['input_ids']
        attention_mask = encoded_input['attention_mask']

        chat_history_ids = model.generate(
            input_ids=input_ids,
            attention_mask=attention_mask,
            max_length=1000,
            pad_token_id=tokenizer.eos_token_id
        )

        response = tokenizer.decode(
            chat_history_ids[:, input_ids.shape[-1]:][0],
            skip_special_tokens=True
        )
        print(f"[DEBUG] Response generated: {response}")
        return response or "Sorry, I couldn't generate a meaningful response."

    except Exception as e:
        print(f"[ERROR] Unexpected error in get_chat_response: {e}")
        return "An error occurred while processing your request."

def is_research_query(text: str) -> bool:
    keywords = ["paper", "research", "article", "abstract", "study", "author", "publication", "findings"]
    return any(k in text.lower() for k in keywords)


if __name__ == '__main__':
    app.run(debug=True)
