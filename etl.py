import boto3
import pandas as pd
import io
import psycopg2
from sqlalchemy import create_engine

# S3 Configuration
S3_BUCKET = 'flight-delay-analytics-varun'
RAW_KEY = 'raw/flights_raw.csv'
PROCESSED_KEY = 'processed/flights_cleaned.csv'

# RDS Configuration (we will fill this in after RDS is created)
DB_HOST = 'flight-delay-db.cxes4k8u8564.us-east-2.rds.amazonaws.com'
DB_NAME = 'flightdb'
DB_USER = 'postgres'
DB_PASS = 'FlightDB2026!'
DB_PORT = '5432'

# ── Step 1: Read raw CSV from S3 ──────────────────────────────────────────────
print("Reading raw data from S3...")
s3 = boto3.client('s3', region_name='us-east-2')
obj = s3.get_object(Bucket=S3_BUCKET, Key=RAW_KEY)
df = pd.read_csv(io.BytesIO(obj['Body'].read()))
print(f"Loaded {len(df)} records from S3")
print(f"Columns: {list(df.columns)}")

# ── Step 2: Clean the data ────────────────────────────────────────────────────
print("\nCleaning data...")

# Keep only relevant columns
cols = [
    'YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK',
    'AIRLINE', 'FLIGHT_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT',
    'SCHEDULED_DEPARTURE', 'DEPARTURE_DELAY', 'ARRIVAL_DELAY',
    'CANCELLED', 'DIVERTED', 'AIR_TIME', 'DISTANCE',
    'WEATHER_DELAY', 'LATE_AIRCRAFT_DELAY', 'AIRLINE_DELAY',
    'SECURITY_DELAY', 'AIR_SYSTEM_DELAY'
]
# Only keep columns that exist in the dataset
cols = [c for c in cols if c in df.columns]
df = df[cols]

# Drop rows where both arrival and departure delay are missing
df = df.dropna(subset=['DEPARTURE_DELAY', 'ARRIVAL_DELAY'], how='all')

# Fill remaining NaN delay columns with 0
delay_cols = ['WEATHER_DELAY', 'LATE_AIRCRAFT_DELAY', 'AIRLINE_DELAY',
              'SECURITY_DELAY', 'AIR_SYSTEM_DELAY']
for col in delay_cols:
    if col in df.columns:
        df[col] = df[col].fillna(0)

# Create a new column: IS_DELAYED (1 if arrival delay > 15 mins)
df['IS_DELAYED'] = (df['ARRIVAL_DELAY'] > 15).astype(int)

# Convert data types
df['DEPARTURE_DELAY'] = pd.to_numeric(df['DEPARTURE_DELAY'], errors='coerce').fillna(0)
df['ARRIVAL_DELAY'] = pd.to_numeric(df['ARRIVAL_DELAY'], errors='coerce').fillna(0)
df['CANCELLED'] = pd.to_numeric(df['CANCELLED'], errors='coerce').fillna(0)

print(f"Records after cleaning: {len(df)}")
print(f"Delayed flights: {df['IS_DELAYED'].sum()}")
print(f"Cancelled flights: {int(df['CANCELLED'].sum())}")

# ── Step 3: Upload cleaned CSV to S3 processed folder ────────────────────────
print("\nUploading cleaned data to S3...")
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
s3.put_object(
    Bucket=S3_BUCKET,
    Key=PROCESSED_KEY,
    Body=csv_buffer.getvalue().encode('utf-8')
)
print(f"Uploaded to s3://{S3_BUCKET}/{PROCESSED_KEY}")

# ── Step 4: Load into RDS PostgreSQL ─────────────────────────────────────────
print("\nLoading data into RDS PostgreSQL...")
engine = create_engine(
    f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

df.to_sql('fact_flights', engine, if_exists='replace', index=False)
print(f"Loaded {len(df)} records into fact_flights table")
print("ETL Complete!")