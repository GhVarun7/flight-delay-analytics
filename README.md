# Flight Delay Analytics — DAMG7370 Final Project

End-to-end data engineering pipeline on AWS

## Student
Varun Gehlot | Northeastern University Seattle | Spring 2026

## Architecture
CSV → Kafka Producer → EC2 Kafka Broker → Consumer → S3 → ETL → RDS PostgreSQL → Streamlit + ML

## AWS Services
- EC2 (t2.micro) — Kafka + ZooKeeper
- S3 (us-east-2) — Data lake (raw/ + processed/)
- RDS PostgreSQL — Star schema warehouse

## Pipeline Scripts
- producer.py — Kafka producer
- consumer.py — Kafka consumer
- etl.py — Data cleaning + RDS load
- build_star_schema.py — Star schema builder
- dashboard.py — Streamlit dashboard
- ml_model.py — Random Forest classifier

## Results
- 97,702 flight records processed
- 34.4% overall delay rate
- 88.63% ML model accuracy
- 82.95% F1 Score
