import pandas as pd
import pickle
from surprise import Dataset, Reader, SVD
from surprise.model_selection import train_test_split
from surprise import accuracy
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

TRAIN_FILE = "/tmp/yelp_training_data.csv"
MODEL_PATH = "/tmp/yelp_svd_model.pkl"

print("Loading training data...")

df = pd.read_csv(TRAIN_FILE)
df = df[["user_id", "business_id", "stars"]].dropna()

print("Rows:", len(df))

reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(df, reader)

trainset, testset = train_test_split(data, test_size=0.2, random_state=42)

print("Training Funk SVD model...")

model = SVD(
    n_factors=50,
    n_epochs=20,
    lr_all=0.005,
    reg_all=0.02
)

model.fit(trainset)

print("Evaluating model...")

predictions = model.test(testset)
rmse = accuracy.rmse(predictions)

print("RMSE:", rmse)

with open(MODEL_PATH, "wb") as f:
    pickle.dump(model, f)

print("Model saved to", MODEL_PATH)