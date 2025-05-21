# 🔍 Malicious/Benign Image Classification

> A real-time image classification system that detects harmful content using deep learning and Apache Kafka for streaming.

## 📌 Overview

This project tackles the problem of moderating user-generated visual content by building a real-time image classification system. Using a fine-tuned **MobileNetV2** model, we classify images into two categories: **harmful** and **benign**. The pipeline integrates PyTorch for model inference and **Apache Kafka** for real-time image streaming.

---

## 🗂 Dataset

- **Source**: [Open Images V7 by Google](https://storage.googleapis.com/openimages/web/download_v7.html)
- **Labels Used**:
  - **Harmful**: gun, knife, blood, explosion
  - **Safe**: person, dog, cat, tree, bicycle, etc.
- Balanced binary classification dataset was created by manually filtering metadata.

📸 Sample Images:

| Harmful | Safe |
|--------|------|
| <img width="327" alt="Harmful" src="https://github.com/user-attachments/assets/1707d2bb-04c3-433f-8b06-f193b55a56a2" /> | <img width="344" alt="Safe" src="https://github.com/user-attachments/assets/b894ed41-d402-4509-81f4-187373eb14b8" /> |

---

## ⚙️ Data Pipeline

- **ETL**:
  - Parsed Open Images CSV metadata using `pandas`
  - Filtered images and organized them into `train/` and `val/` directories
- **Preprocessing**:
  - Resize: 224×224  
  - Normalize: ImageNet mean/std  
  - Tools: `torchvision.transforms`, `ImageFolder`

---

## 🧠 Models

We compared two models:

| Model        | Accuracy | Precision (Harmful) | Recall (Harmful) | AUC |
|--------------|----------|---------------------|------------------|-----|
| **MobileNetV2** | 83.5%    | 0.87                | 0.79             | 0.92 |
| **ResNet50**    | 82.2%    | 0.96                | 0.68             | 0.93 |

📊 Confusion Matrix:

<img width="743" alt="Screenshot 2025-05-21 at 9 54 54 AM" src="https://github.com/user-attachments/assets/15db36c0-7faf-4691-bea7-a3f877c62501" />

🔍 **Conclusion**: MobileNetV2 had better recall, making it more reliable for safety-critical applications.

---

## 📡 Streaming with Apache Kafka

- Kafka producer reads local image paths and sends metadata to the Kafka topic.
- Kafka consumer loads and classifies each image in real time using MobileNetV2.
- Achieved <300ms latency per image on CPU-only systems.

## 🖥️ Example Output:

```bash
[image_0] Path: ./images/gun.jpg -> harmful
[image_1] Path: ./images/dog.jpg -> safe
```
## Resources:
📖 Open Images Dataset V7 (Google AI):
https://storage.googleapis.com/openimages/web/download_v7.html

📁 Demo Image Folder:
https://drive.google.com/drive/folders/1404MArZCvH78d9UxaUGctLcUZ1rQjhah?usp=sharing

📁 Model Folder:
https://drive.google.com/drive/folders/1d6dlMSfdFAvN5FRN9hJIU55pWAO-H4-U?usp=drive_link

📁 Training & Testing Dataset:
https://drive.google.com/drive/folders/15_WRlkWXn0nEOQSQ3Ct_1LLsbo6UpELU?usp=drive_link
