from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pymongo
import json
import os
import pandas as pd
import subprocess
import sys
from google.cloud import storage
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "yelp_db")

COLLECTION_RAW = "raw_reviews"
COLLECTION_AGG = "business_star_stats"
COLLECTION_YEARLY = "yearly_star_stats"

BUCKET_NAME = os.getenv("BUCKET_NAME", "msds697_les_buckettes_du_yelp")
BLOB_NAME = os.getenv(
    "BLOB_NAME",
    "Yelp_JSON/yelp_dataset/yelp_academic_dataset_review.json"
)

TRAIN_FILE = "/tmp/yelp_training_data.csv"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

TRAIN_SCRIPT = os.getenv("TRAIN_SCRIPT", str(SRC_DIR / "train_funk_svd.py"))
RECS_SCRIPT = os.getenv("RECS_SCRIPT", str(SRC_DIR / "generate_recommendations.py"))

MAX_REVIEWS = int(os.getenv("MAX_REVIEWS", "100000"))


def get_mongo_client():
    if not MONGO_URI:
        raise ValueError("MONGO_URI environment variable is not set.")
    return pymongo.MongoClient(MONGO_URI)


def load_data_to_mongo():
    mongo_client = get_mongo_client()
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_RAW]

    print("Downloading Yelp review data from Google Cloud Storage...")

    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)

    data_buffer = []

    with blob.open("r") as f:
        for i, line in enumerate(f):
            if i >= MAX_REVIEWS:
                break
            data_buffer.append(json.loads(line))

    if not data_buffer:
        raise ValueError("No data was loaded from the GCS file.")

    collection.delete_many({})
    collection.insert_many(data_buffer)

    print(f"Inserted {len(data_buffer)} documents into {COLLECTION_RAW}")


def create_business_star_stats():
    client = get_mongo_client()
    db = client[DB_NAME]

    pipeline = [
        {
            "$group": {
                "_id": "$business_id",
                "average_stars": {"$avg": "$stars"},
                "review_count": {"$sum": 1},
                "average_useful": {"$avg": "$useful"},
                "average_funny": {"$avg": "$funny"},
                "average_cool": {"$avg": "$cool"}
            }
        },
        {"$sort": {"review_count": -1}},
        {"$out": COLLECTION_AGG}
    ]

    list(db[COLLECTION_RAW].aggregate(pipeline))
    print(f"Created {COLLECTION_AGG}")


def create_yearly_star_stats():
    client = get_mongo_client()
    db = client[DB_NAME]

    pipeline = [
        {
            "$project": {
                "business_id": 1,
                "stars": 1,
                "year": {"$year": {"$toDate": "$date"}}
            }
        },
        {
            "$group": {
                "_id": {
                    "business_id": "$business_id",
                    "year": "$year"
                },
                "yearly_avg_stars": {"$avg": "$stars"},
                "yearly_review_count": {"$sum": 1}
            }
        },
        {"$sort": {"_id.year": 1, "yearly_review_count": -1}},
        {"$out": COLLECTION_YEARLY}
    ]

    list(db[COLLECTION_RAW].aggregate(pipeline))
    print(f"Created {COLLECTION_YEARLY}")


def export_training_data():
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_RAW]

    rows = []

    for doc in collection.find({}, {"user_id": 1, "business_id": 1, "stars": 1, "_id": 0}):
        rows.append({
            "user_id": doc.get("user_id"),
            "business_id": doc.get("business_id"),
            "stars": doc.get("stars")
        })

    df = pd.DataFrame(rows).dropna()

    if df.empty:
        raise ValueError("Training data export is empty.")

    df.to_csv(TRAIN_FILE, index=False)

    print(f"Exported {len(df)} rows to {TRAIN_FILE}")


def train_recommender_model():
    result = subprocess.run([sys.executable, TRAIN_SCRIPT], check=False)
    if result.returncode != 0:
        raise RuntimeError("Funk SVD training failed")
    print("Funk SVD training completed")


def generate_and_store_recommendations():
    result = subprocess.run([sys.executable, RECS_SCRIPT], check=False)
    if result.returncode != 0:
        raise RuntimeError("Recommendation generation failed")
    print("Recommendation generation completed")


default_args = {
    "owner": "yelp_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="yelp_processing_pipeline_final",
    default_args=default_args,
    description="Yelp recommender pipeline with GCS -> MongoDB Atlas -> Funk SVD",
    schedule="@once",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="load_reviews_to_mongo",
        python_callable=load_data_to_mongo,
    )

    t2 = PythonOperator(
        task_id="create_business_star_stats",
        python_callable=create_business_star_stats,
    )

    t3 = PythonOperator(
        task_id="create_yearly_star_stats",
        python_callable=create_yearly_star_stats,
    )

    t4 = PythonOperator(
        task_id="export_training_data",
        python_callable=export_training_data,
    )

    t5 = PythonOperator(
        task_id="train_recommender_model",
        python_callable=train_recommender_model,
    )

    t6 = PythonOperator(
        task_id="generate_and_store_recommendations",
        python_callable=generate_and_store_recommendations,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6