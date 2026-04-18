import os
import json
import time

import pandas as pd
from kafka import KafkaProducer


KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'instagram-users')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
DELAY_SECONDS = float(os.getenv('DELAY_SECONDS', 2))
CSV_FILE = os.getenv('CSV_FILE', 'social_media_users.csv')

print(f"Producer Configuration:")
print(f"  Kafka Servers: {KAFKA_SERVERS}")
print(f"  Topic: {KAFKA_TOPIC}")
print(f"  Batch Size: {BATCH_SIZE}")
print(f"  Delay: {DELAY_SECONDS}s")
print(f"  CSV File: {CSV_FILE}")


try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_request_size=10485760,
        linger_ms=10,
    )
    print("Connected to Kafka successfully")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    exit(1)

try:
    df = pd.read_csv(os.path.join("/app", "data", CSV_FILE))
    records = json.loads(df.to_json(orient='records'))
    print(f"Loaded {len(records)} records from {CSV_FILE}")
except Exception as e:
    print(f"Failed to load CSV: {e}")
    exit(1)

cycle_count = 0
total_sent = 0

try:
    while True:
        cycle_count += 1
        print(f"\n{'='*50}")
        print(f"Starting Cycle {cycle_count}")
        print(f"{'='*50}")
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            
            for record in batch:
                producer.send(KAFKA_TOPIC, value=record)
                total_sent += 1
            
            producer.flush()
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"Batch {batch_num}/{total_batches}: {len(batch)} records | Total: {total_sent}")
            
            time.sleep(DELAY_SECONDS)
        
        print(f"Cycle {cycle_count} completed, restarting...")
except Exception as e:
    print(f"\nUnexpected error: {e}")
    producer.close()
    exit(1)
