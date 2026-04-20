import json
import boto3
import csv
import io
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BROKER = '3.21.126.255:9092'
TOPIC = 'flight-delays'

# S3 configuration
S3_BUCKET = 'flight-delay-analytics-varun'
S3_KEY = 'raw/flights_raw.csv'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Initialize S3 client
s3 = boto3.client('s3', region_name='us-east-2')

print("Kafka Consumer started...")
print(f"Listening to topic: {TOPIC}")

# Collect messages in memory
messages = []
count = 0

for message in consumer:
    row = message.value
    messages.append(row)
    count += 1

    # Print progress every 1000 records
    if count % 1000 == 0:
        print(f"Received {count} records...")

    # Once we have 100,000 records, save to S3 and stop
    if count >= 100000:
        print("100,000 records received. Saving to S3...")
        break

# Convert to CSV format in memory
if messages:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=messages[0].keys())
    writer.writeheader()
    writer.writerows(messages)
    csv_content = output.getvalue()

    # Upload to S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY,
        Body=csv_content.encode('utf-8')
    )
    print(f"Successfully uploaded {count} records to s3://{S3_BUCKET}/{S3_KEY}")

print("Consumer finished!")