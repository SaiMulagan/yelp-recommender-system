import os
from fastapi import FastAPI
from pymongo import MongoClient
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "yelp_db")
COLLECTION_RECS = "recommendations"

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set.")

app = FastAPI(title="Yelp Recommender API")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
rec_collection = db[COLLECTION_RECS]


@app.get("/")
def home():
    return {"message": "Yelp Recommender API is running"}


@app.get("/recommendations/{user_id}")
def get_recommendations(user_id: str):
    doc = rec_collection.find_one({"user_id": user_id}, {"_id": 0})

    if not doc:
        return {"user_id": user_id, "recommendations": []}

    return doc