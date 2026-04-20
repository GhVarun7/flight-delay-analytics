import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (accuracy_score, precision_score,
                             recall_score, f1_score,
                             confusion_matrix, classification_report)
from sklearn.preprocessing import LabelEncoder
import pickle
import os

# ── DB CONFIG ──────────────────────────────────────────────────────────────
DB_HOST = "flight-delay-db.cxes4k8u8564.us-east-2.rds.amazonaws.com"
DB_NAME = "flightdb"
DB_USER = "postgres"
DB_PASS = "FlightDB2026!"
DB_PORT = 5432

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ── LOAD DATA ──────────────────────────────────────────────────────────────
print("Loading data from RDS...")
df = pd.read_sql("""
    SELECT
        f."AIRLINE",
        f."ORIGIN_AIRPORT",
        f."DESTINATION_AIRPORT",
        f."DAY_OF_WEEK",
        f."MONTH",
        f."DISTANCE",
        f."DEPARTURE_DELAY",
        f."AIR_TIME",
        f."IS_DELAYED"
    FROM fact_flights f
""", engine)

print(f"  Loaded {len(df)} rows.")

# ── FEATURE ENGINEERING ────────────────────────────────────────────────────
print("\nPreparing features...")

# Drop rows where target is null
df = df.dropna(subset=["IS_DELAYED"])

# Fill nulls in numeric features with 0
df["DEPARTURE_DELAY"] = df["DEPARTURE_DELAY"].fillna(0)
df["AIR_TIME"]        = df["AIR_TIME"].fillna(df["AIR_TIME"].median())
df["DISTANCE"]        = df["DISTANCE"].fillna(df["DISTANCE"].median())

# Encode categorical columns
le_airline  = LabelEncoder()
le_origin   = LabelEncoder()
le_dest     = LabelEncoder()

df["AIRLINE_ENC"]  = le_airline.fit_transform(df["AIRLINE"].astype(str))
df["ORIGIN_ENC"]   = le_origin.fit_transform(df["ORIGIN_AIRPORT"].astype(str))
df["DEST_ENC"]     = le_dest.fit_transform(df["DESTINATION_AIRPORT"].astype(str))

# Feature columns
FEATURES = [
    "AIRLINE_ENC",
    "ORIGIN_ENC",
    "DEST_ENC",
    "DAY_OF_WEEK",
    "MONTH",
    "DISTANCE",
    "DEPARTURE_DELAY",
    "AIR_TIME"
]

X = df[FEATURES]
y = df["IS_DELAYED"].astype(int)

print(f"  Features: {FEATURES}")
print(f"  Class balance — Delayed: {y.sum()} ({y.mean()*100:.1f}%)  On-time: {(~y.astype(bool)).sum()}")

# ── TRAIN / TEST SPLIT ─────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"\nTrain size: {len(X_train)}  |  Test size: {len(X_test)}")

# ── TRAIN MODEL ────────────────────────────────────────────────────────────
print("\nTraining Random Forest... (this may take ~1-2 minutes)")
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=12,
    min_samples_split=10,
    class_weight="balanced",
    random_state=42,
    n_jobs=-1      # use all CPU cores
)
model.fit(X_train, y_train)
print("  Training complete.")

# ── EVALUATE ───────────────────────────────────────────────────────────────
print("\nEvaluating model...")
y_pred = model.predict(X_test)

acc  = accuracy_score(y_test, y_pred)
prec = precision_score(y_test, y_pred)
rec  = recall_score(y_test, y_pred)
f1   = f1_score(y_test, y_pred)
cm   = confusion_matrix(y_test, y_pred)

print("\n" + "="*45)
print("         MODEL EVALUATION RESULTS")
print("="*45)
print(f"  Accuracy  : {acc*100:.2f}%")
print(f"  Precision : {prec*100:.2f}%")
print(f"  Recall    : {rec*100:.2f}%")
print(f"  F1 Score  : {f1*100:.2f}%")
print("\n  Confusion Matrix:")
print(f"              Predicted")
print(f"              On-Time  Delayed")
print(f"  Actual On-Time  {cm[0][0]:>6}   {cm[0][1]:>6}")
print(f"  Actual Delayed  {cm[1][0]:>6}   {cm[1][1]:>6}")
print("="*45)

print("\n  Full Classification Report:")
print(classification_report(y_test, y_pred, target_names=["On-Time", "Delayed"]))

# ── FEATURE IMPORTANCE ─────────────────────────────────────────────────────
print("\nFeature Importances:")
feat_imp = pd.DataFrame({
    "Feature": FEATURES,
    "Importance": model.feature_importances_
}).sort_values("Importance", ascending=False)

for _, row in feat_imp.iterrows():
    bar = "█" * int(row["Importance"] * 100)
    print(f"  {row['Feature']:<20} {row['Importance']:.4f}  {bar}")

# ── SAVE MODEL & ENCODERS ──────────────────────────────────────────────────
print("\nSaving model and encoders...")

save_path = r"C:\Users\gehlo\OneDrive\Desktop\DAMG Project"

with open(os.path.join(save_path, "flight_delay_model.pkl"), "wb") as f:
    pickle.dump(model, f)

with open(os.path.join(save_path, "encoders.pkl"), "wb") as f:
    pickle.dump({
        "airline":  le_airline,
        "origin":   le_origin,
        "dest":     le_dest,
        "features": FEATURES
    }, f)

print(f"  Saved: flight_delay_model.pkl")
print(f"  Saved: encoders.pkl")
print("\n✅ ML model complete! Ready to use in dashboard.")

# ── SAVE METRICS FOR DASHBOARD ─────────────────────────────────────────────
metrics = {
    "accuracy":  round(acc * 100, 2),
    "precision": round(prec * 100, 2),
    "recall":    round(rec * 100, 2),
    "f1":        round(f1 * 100, 2),
    "confusion_matrix": cm.tolist(),
    "feature_importance": feat_imp.to_dict(orient="records")
}

import json
with open(os.path.join(save_path, "model_metrics.json"), "w") as f:
    json.dump(metrics, f)

print("  Saved: model_metrics.json")