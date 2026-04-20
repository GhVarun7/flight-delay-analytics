import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# --- DB CONFIG ---
DB_HOST = "flight-delay-db.cxes4k8u8564.us-east-2.rds.amazonaws.com"
DB_NAME = "flightdb"
DB_USER = "postgres"
DB_PASS = "FlightDB2026!"
DB_PORT = 5432

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
cur = conn.cursor()

print("Connected to RDS.")

# ── 1. LOAD fact_flights into a dataframe ──────────────────────────────────
print("Loading fact_flights...")
df = pd.read_sql("SELECT * FROM fact_flights", engine)
print(f"  {len(df)} rows loaded.")

# ══════════════════════════════════════════════════════
# DIM_AIRLINE
# ══════════════════════════════════════════════════════
print("\nBuilding dim_airline...")

# Airline code → full name mapping (BTS codes)
airline_names = {
    "UA": "United Air Lines",
    "AA": "American Airlines",
    "US": "US Airways",
    "F9": "Frontier Airlines",
    "B6": "JetBlue Airways",
    "OO": "SkyWest Airlines",
    "AS": "Alaska Airlines",
    "NK": "Spirit Airlines",
    "WN": "Southwest Airlines",
    "DL": "Delta Air Lines",
    "EV": "ExpressJet Airlines",
    "HA": "Hawaiian Airlines",
    "MQ": "American Eagle Airlines",
    "VX": "Virgin America",
}

airlines = df[["AIRLINE"]].drop_duplicates().rename(columns={"AIRLINE": "airline_code"})
airlines["airline_name"] = airlines["airline_code"].map(airline_names).fillna("Unknown")
airlines = airlines.reset_index(drop=True)
airlines.index += 1
airlines.index.name = "airline_key"
airlines = airlines.reset_index()

cur.execute("DROP TABLE IF EXISTS dim_airline CASCADE;")
cur.execute("""
    CREATE TABLE dim_airline (
        airline_key  SERIAL PRIMARY KEY,
        airline_code VARCHAR(10) NOT NULL,
        airline_name VARCHAR(100)
    );
""")
conn.commit()

for _, row in airlines.iterrows():
    cur.execute(
        "INSERT INTO dim_airline (airline_code, airline_name) VALUES (%s, %s)",
        (row["airline_code"], row["airline_name"])
    )
conn.commit()
print(f"  dim_airline: {len(airlines)} rows inserted.")

# ══════════════════════════════════════════════════════
# DIM_AIRPORT
# ══════════════════════════════════════════════════════
print("\nBuilding dim_airport...")

origins = df[["ORIGIN_AIRPORT"]].rename(columns={"ORIGIN_AIRPORT": "airport_code"})
dests   = df[["DESTINATION_AIRPORT"]].rename(columns={"DESTINATION_AIRPORT": "airport_code"})
airports = pd.concat([origins, dests]).drop_duplicates().reset_index(drop=True)

# Filter to IATA codes only (3 uppercase letters)
airports = airports[airports["airport_code"].str.match(r'^[A-Z]{3}$', na=False)].copy()
airports["airport_name"] = airports["airport_code"]  # placeholder — no lookup file available
airports["city"]         = "Unknown"
airports["state"]        = "Unknown"

cur.execute("DROP TABLE IF EXISTS dim_airport CASCADE;")
cur.execute("""
    CREATE TABLE dim_airport (
        airport_key  SERIAL PRIMARY KEY,
        airport_code VARCHAR(10) NOT NULL,
        airport_name VARCHAR(100),
        city         VARCHAR(100),
        state        VARCHAR(50)
    );
""")
conn.commit()

for _, row in airports.iterrows():
    cur.execute(
        "INSERT INTO dim_airport (airport_code, airport_name, city, state) VALUES (%s, %s, %s, %s)",
        (row["airport_code"], row["airport_name"], row["city"], row["state"])
    )
conn.commit()
print(f"  dim_airport: {len(airports)} rows inserted.")

# ══════════════════════════════════════════════════════
# DIM_DATE
# ══════════════════════════════════════════════════════
print("\nBuilding dim_date...")

import calendar

date_rows = df[["YEAR", "MONTH", "DAY", "DAY_OF_WEEK"]].drop_duplicates().copy()
date_rows = date_rows.dropna().astype(int)

def get_season(month):
    if month in [12, 1, 2]:  return "Winter"
    elif month in [3, 4, 5]: return "Spring"
    elif month in [6, 7, 8]: return "Summer"
    else:                     return "Fall"

date_rows["date_key"]   = (date_rows["YEAR"].astype(str)
                           + date_rows["MONTH"].astype(str).str.zfill(2)
                           + date_rows["DAY"].astype(str).str.zfill(2)).astype(int)
date_rows["full_date"]  = pd.to_datetime(date_rows[["YEAR","MONTH","DAY"]].rename(
                            columns={"YEAR":"year","MONTH":"month","DAY":"day"}))
date_rows["month_name"] = date_rows["MONTH"].apply(lambda m: calendar.month_name[m])
date_rows["day_name"]   = date_rows["full_date"].dt.day_name()
date_rows["quarter"]    = ((date_rows["MONTH"] - 1) // 3) + 1
date_rows["is_weekend"] = date_rows["DAY_OF_WEEK"].isin([6, 7])
date_rows["season"]     = date_rows["MONTH"].apply(get_season)

date_rows = date_rows.drop_duplicates(subset="date_key")

cur.execute("DROP TABLE IF EXISTS dim_date CASCADE;")
cur.execute("""
    CREATE TABLE dim_date (
        date_key   INT PRIMARY KEY,
        full_date  DATE,
        year       INT,
        quarter    INT,
        month      INT,
        month_name VARCHAR(20),
        day        INT,
        day_of_week INT,
        day_name   VARCHAR(20),
        is_weekend BOOLEAN,
        season     VARCHAR(10)
    );
""")
conn.commit()

for _, row in date_rows.iterrows():
    cur.execute("""
        INSERT INTO dim_date
          (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend, season)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        int(row["date_key"]), row["full_date"].date(),
        int(row["YEAR"]), int(row["quarter"]), int(row["MONTH"]),
        row["month_name"], int(row["DAY"]), int(row["DAY_OF_WEEK"]),
        row["day_name"], bool(row["is_weekend"]), row["season"]
    ))
conn.commit()
print(f"  dim_date: {len(date_rows)} rows inserted.")

# ══════════════════════════════════════════════════════
# DONE
# ══════════════════════════════════════════════════════
cur.close()
conn.close()
print("\n Star schema complete! Tables created: dim_airline, dim_airport, dim_date")