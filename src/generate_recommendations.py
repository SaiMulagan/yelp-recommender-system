import os
import pandas as pd
import pickle
import pymongo
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TRAIN_FILE = "/tmp/yelp_training_data.csv"
MODEL_PATH = "/tmp/yelp_svd_model.pkl"

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "yelp_db")
COLLECTION_RECS = "recommendations"

if not MONGO_URI:
    raise ValueError("MONGO_URI environment variable is not set.")

print("Loading training data...")
df = pd.read_csv(TRAIN_FILE)
df = df[["user_id", "business_id", "stars"]].dropna()

print("Loading trained model...")
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

users = df["user_id"].unique().tolist()
businesses = df["business_id"].unique().tolist()

print("Connecting to MongoDB Atlas...")
client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
rec_collection = db[COLLECTION_RECS]

rec_collection.delete_many({})

results = []

max_users = min(100, len(users))
max_businesses = min(500, len(businesses))

print(f"Generating recommendations for {max_users} users...")

for user in users[:max_users]:
    seen = set(df[df["user_id"] == user]["business_id"].tolist())
    scores = []

    for biz in businesses[:max_businesses]:
        if biz in seen:
            continue

        pred = model.predict(user, biz)
        scores.append({
            "business_id": biz,
            "score": float(pred.est)
        })

    top_recs = sorted(scores, key=lambda x: x["score"], reverse=True)[:10]

    results.append({
        "user_id": user,
        "recommendations": top_recs
    })

pipeline = [
    {
        '$lookup': {
            'from': 'raw_reviews', 
            'localField': 'user_id', 
            'foreignField': 'user_id', 
            'as': 'Usersreviews'
        }
    }, {
        '$lookup': {
            'from': 'yelp_businesses', 
            'localField': 'Usersreviews.business_id', 
            'foreignField': 'business_id', 
            'as': 'patronizedbusiness'
        }
    }, {
        '$addFields': {
            'city': '$patronizedbusiness.city'
        }
    }, {
        '$unwind': {
            'path': '$city'
        }
    }, {
        '$group': {
            '_id': {
                'user': '$user_id', 
                'city': '$city'
            }, 
            'total': {
                '$sum': 1
            }
        }
    }, {
        '$group': {
            '_id': '$_id.user', 
            'terms': {
                '$push': {
                    'term': '$_id.city', 
                    'total': '$total'
                }
            }, 
            'location': {
                '$top': {
                    'sortBy': {
                        'total': -1
                    }, 
                    'output': '$_id.city'
                }
            }
        }
    }, {
        '$project': {
            '_id': 1, 
            'location': '$location'
        }
    }, {
        '$lookup': {
            'from': 'recommendations', 
            'localField': '_id', 
            'foreignField': 'user_id', 
            'as': 'recommendations'
        }
    }, {
        '$lookup': {
            'from': 'yelp_businesses', 
            'localField': 'recommendations.recommendations.business_id', 
            'foreignField': 'business_id', 
            'as': 'recommendations.recommendations.business_id.location'
        }
    }, {
        '$addFields': {
            'recommendations': {
                '$filter': {
                    'input': '$recommendations.recommendations.business_id.location', 
                    'as': 'business', 
                    'cond': {
                        '$eq': [
                            '$$business.city', '$location'
                        ]
                    }
                }
            }
        }
    }, {
        '$out': {
            'db': 'yelp_db', 
            'coll': 'location-limitedrecommendations'
        }
    }
]


if results:
    rec_collection.insert_many(results)

rec_collection.aggregate(pipeline)

print(f"Stored recommendations for {len(results)} users in MongoDB. Also stored location-limited recommendations.")
