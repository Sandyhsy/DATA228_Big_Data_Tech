from kafka import KafkaProducer
import json
import os
import time

# Folder containing your images
IMAGE_FOLDER = "./images"

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Loop through all image files in the folder
for idx, filename in enumerate(os.listdir(IMAGE_FOLDER)):
    if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
        full_path = os.path.join(IMAGE_FOLDER, filename)

        message = {
            'image_id': f'image_{idx}',
            'image_path': full_path
        }

        producer.send('image-data', value=message)
        print(f"Sent: {message}")
        time.sleep(0.1)

producer.flush()
producer.close()
