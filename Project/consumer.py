from kafka import KafkaConsumer
import json
import requests
from PIL import Image
from io import BytesIO
import torch
from torchvision import transforms, models
from torchvision.models import mobilenet_v2

model = mobilenet_v2(pretrained=False)
model.classifier[1] = torch.nn.Linear(model.last_channel, 2)
model.load_state_dict(torch.load("mobilenetv2_simple.pth", map_location=torch.device("cpu")))
model.eval()
# Preprocessing
transform = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize(
        mean=[0.485, 0.456, 0.406],
        std=[0.229, 0.224, 0.225]
    )
])

# Predict from image URL
def predict_image_from_path(image_path):
    try:
        img = Image.open(image_path).convert('RGB')
        input_tensor = transform(img).unsqueeze(0)
        with torch.no_grad():
            output = model(input_tensor)
            _, predicted = torch.max(output, 1)
            return "safe" if predicted.item() == 0 else "harmful"
    except FileNotFoundError:
        return "pic not found"
    except Exception as e:
        return f"Error: {e}"

# Kafka Consumer setup
consumer = KafkaConsumer(
    'image-data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='image_classifier_NEW',     # <— Use a new group ID
    auto_offset_reset='latest'           # <— Only read new messages from now on
)


# Start consuming messages
for message in consumer:
    data = message.value
    image_id = data['image_id']
    image_path = data['image_path']
    result = predict_image_from_path(image_path)
    print(f"[{image_id}] Path: {image_path} -> {result}")
