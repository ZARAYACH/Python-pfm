import pandas as pd
import re
import unicodedata
from dbUtil import engine


def clean_text(text):
    if pd.isnull(text):
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8")
    text = re.sub(r"[^\w\s\-.,:/]", "", text)
    return re.sub(r"\s+", " ", text).strip()

def normalize_author(name):
    name = clean_text(name)
    return name.title()
