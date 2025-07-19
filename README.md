# Scopus NLP Chatbot – Data Ingestion Pipeline

This project is part of a larger system designed to build an intelligent chatbot capable of understanding and retrieving academic data from Scopus. This `README.md` focuses on the **data ingestion pipeline**, which processes and stores metadata from downloaded Scopus articles efficiently using a threaded architecture.

--- 

# Project Overview

The ingestion pipeline processes large datasets of scientific articles (in JSON format) and stores their metadata (title, abstract, authors, publication date, etc.) in a relational database. It is designed to be efficient, scalable, and avoid concurrent database write issues by using a **producer-consumer pattern** with threads and a shared queue.

---

## Features

- Parses Scopus/ArXiv-like metadata from JSON
- iInserts data into relational database using SQLAlchemy
- Handles authors, articles, and their many-to-many relations
- Uses a dedicated writer thread to avoid race conditions
- Deduplicates entries via `INSERT IGNORE` or `ON DUPLICATE KEY UPDATE`
- Supports large datasets by processing in chunks

---

## Architecture

### Main Components

1. **Reader Threads (Producer):**
   - Reads JSON file and parses article info
   - Prepares bulk insert data for articles and authors
   - Places data into a shared `Queue`

2. **Writer Thread (Consumer):**
   - Continuously listens to the queue
   - Inserts articles and authors into the DB
   - Links authors to articles via an associative table
   - Catches and logs DB errors

---

## Technologies

- **Python 3**articles
- **SQLAlchemy** (Core mode for speed)
- **MySQL / MariaDB** (with support for `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE`)
- **Threading & Queue** (to avoid concurrent DB writes)
- **JSON** (as article data input format)

---

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/ZARAYACH/Python-pfm.git
   cd Python-pfm```
2. Create a virtual environment and install dependencies:
   ```
   python -m venv venv
   source venv/bin/activate #depends on your os i'm using linux
   pip install -r requirements.txt 
   ```
3. Download data from google drive 
   - Download data zip file and extract in data folder (This is arxiv db snapshot it is used to generate the index file and also to populate the db ): https://drive.google.com/file/d/17Hb-vM5DeEeGhSNAASEqwEfkkM_2WPEN/view?usp=drive_link
   - Download a db sample and execute the sql file against your db , to populate the db. it has roughly 160000 articles : https://drive.google.com/file/d/10jTA1t7aUCRECV3wJyc8j8tv8xYRlk4y/view?usp=drive_link
   - please if you run this script make sure to limit the ram usage and adjust the max threads number and batche size or it will cause system crashes and other unwanted behaviours
4. For an already generated index file for the provided db you can download and extract the file from and put int 'out' folder ':https://drive.google.com/file/d/1pHWY_c7oTZmA2D9CldkBzKjqSoflFBmf/view?usp=drive_link 
5. edit dbUtils env var to connect to your MySQL database. 
---
## Usage

You can download the full arxiv data from https://www.kaggle.com/datasets/Cornell-University/arxiv/suggestions

Run the ingestion script(don't forget to change the file path if you have a different file name):

python arxiv_extractor.py 

## Database Schema

articles (id, title, abstract, published, arxiv_id, categories, doi)

authors (id, name)

article_authors (article_id, author_id) — linking table

Indexes are used on arxiv_id and name to speed up lookups
