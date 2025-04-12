from kafka import KafkaProducer
import pandas as pd
import pyarrow.parquet as pq
import json
import time

# Read and clean the data
yellow_trips = pq.read_table('yellow_tripdata_2025-01.parquet')
yellow_trips = yellow_trips.to_pandas()

# Data cleaning process
yellow_trips = yellow_trips[yellow_trips['tpep_pickup_datetime'] <= yellow_trips['tpep_dropoff_datetime']]
yellow_trips = yellow_trips[yellow_trips['fare_amount'] > 0]

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize records as JSON
)

# Produce data to Kafka topic
for index, record in yellow_trips.iterrows():
    message = {
        'fare_amount': record['fare_amount'],
        'pickup_datetime': record['tpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        'dropoff_datetime': record['tpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        'passenger_count': record['passenger_count']
    }
    
    # Send message to Kafka topic "taxi_trip_data"
    producer.send('taxi_trip_data', value=message)
    
    # Print to verify
    print(f"Sent: {message}")
    
    # Add a delay between sending records (optional, to simulate real-time processing)
    time.sleep(0.1)

producer.flush()  # Ensure all messages are sent before closing
producer.close()