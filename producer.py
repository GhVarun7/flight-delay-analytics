import csv
import json
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = '3.21.126.255:9092'
TOPIC = 'flight-delays'

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Kafka Producer started...")
print(f"Sending flight data to topic: {TOPIC}")

# Read and send flights.csv
count = 0
with open('flights.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Only send rows with delay information
        producer.send(TOPIC, value=dict(row))
        count += 1
        
        # Print progress every 1000 records
        if count % 1000 == 0:
            print(f"Sent {count} records...")
        
        # Stop after 100,000 records (enough for demo)
        if count >= 100000:
            break
        
        # Small delay to simulate streaming
        time.sleep(0.001)

producer.flush()
print(f"Done! Total records sent: {count}")