# Flight Delay Analytics - DAMG7370 Final Project

End-to-end cloud-based data engineering pipeline for flight delay 
analysis and prediction, built on AWS.

**Course:** DAMG 7370 — Designing Data Architecture for Business Intelligence  
**Student:** Varun Gehlot | Northeastern University Seattle | Spring 2026


## Architecture
CSV → Kafka Producer → EC2 Kafka Broker → Consumer → S3 raw/
→ ETL → S3 processed/ → RDS PostgreSQL (Star Schema)
→ Streamlit Dashboard + Random Forest ML Model

---

## Repository Structure
flight-delay-analytics/
├── producer.py          # Kafka producer - streams flights.csv to topic
├── consumer.py          # Kafka consumer - writes to S3 raw/
├── etl.py               # ETL — cleans data, loads to RDS fact_flights
├── build_star_schema.py # Builds dim_airline, dim_airport, dim_date
├── dashboard.py         # Streamlit dashboard - 3 sections, 8 charts
└── ml_model.py          # Random Forest delay classifier


---

## AWS Services

| Service | Resource | Purpose |
|---------|----------|---------|
| EC2 (t2.micro) | kafka-flight-se... | Kafka 3.6.1 + ZooKeeper broker |
| S3 (us-east-2) | flight-delay-analytics-varun | Data lake — raw/ + processed/ |
| RDS PostgreSQL | flight-delay-db | Star schema warehouse |

---

## Data Pipeline

**Data Source:** BTS Flight On-Time Performance Data 2015  
- 5.8M rows, 578MB raw CSV  
- 100,000 records streamed via Kafka  
- 97,702 clean records after ETL  

**Star Schema (Kimball):**
- `fact_flights` — 97,702 rows
- `dim_airline` — 14 airlines
- `dim_airport` — 312 airports
- `dim_date` — 7 dates with season + weekend flags

---

## Dashboard

Streamlit dashboard with 3 sections:
- **Section 1** — Flight Delay Trends (airline, day of week, departure hour)
- **Section 2** — Operational Analysis (airports, delay causes, worst routes)
- **Section 3** — What-If Delay Predictor (live ML prediction with gauge)

---

## ML Model

**Algorithm:** Random Forest Classifier (scikit-learn)  
**Features:** Airline, origin/destination airport, day of week, 
month, distance, air time  

| Metric | Score |
|--------|-------|
| Accuracy | 88.63% |
| Precision | 85.65% |
| Recall | 80.42% |
| F1 Score | 82.95% |

**Key finding:** Departure delay accounts for 81.5% of the feature 
importance - cascading delays are the dominant pattern.

---

## Setup

**Requirements:**
pip install kafka-python boto3 pandas numpy scikit-learn
streamlit psycopg2-binary sqlalchemy

**Run pipeline:**
```bash
# Start consumer first
python consumer.py

# Then start producer
python producer.py
```

**Run ETL:**
```bash
python etl.py
```

**Build star schema:**
```bash
python build_star_schema.py
```

**Run dashboard:**
```bash
streamlit run dashboard.py
```

---

## Key Findings

- 34.4% overall delay rate across 97,702 flights
- American Eagle worst airline at 56.4% delay rate
- Late Aircraft #1 delay cause - 900,754 total minutes
- JFK → EGE worst route at 199 min average delay
- Weekends (45.9%) are significantly worse than weekdays (29%)
- Sunday worst day (48%) vs Thursday best day (21%)

---

## Final Report

GitHub: https://github.com/GhVarun7/flight-delay-analytics  
Email: gehlot.v@northeastern.edu
