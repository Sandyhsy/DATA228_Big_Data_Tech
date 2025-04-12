from kafka import KafkaConsumer
import json
from collections import deque
from datetime import datetime

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'taxi_trip_data',  # Kafka topic name
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='taxi_group'
)

# Initialize a deque to store data for calculating average fare within a 30-minute window
fare_window = deque()
window_duration = 30 * 60  # 30 minutes in seconds

def calculate_average_fare(window_data):
    if not window_data:
        return 0
    total_fare = sum([record[1] for record in window_data])  # record[1] is fare_amount
    return total_fare / len(window_data)

# Consumer behavior: Processing the streamed data from Kafka
for message in consumer:
    record = message.value
    fare_amount = record['fare_amount']
    pickup_datetime = datetime.strptime(record['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
    
    # Add new record to the fare_window
    fare_window.append((pickup_datetime, fare_amount))
    
    # Remove records that are older than the 30-minute window
    while fare_window and (pickup_datetime - fare_window[0][0]).total_seconds() > window_duration:
        fare_window.popleft()

    # Calculate and display the average fare for the current window
    avg_fare = calculate_average_fare(fare_window)
    print(f"Current Pickup Time: {pickup_datetime} | Average fare for the last 30 minutes: ${avg_fare:.2f}")
