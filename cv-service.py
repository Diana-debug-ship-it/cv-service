import json
import os
import time
import random  # <-- Добавили модуль для рандома
from kafka import KafkaProducer
from ultralytics import YOLO

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'frame.check.results'

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

model = YOLO("yolov8n.pt")

print("CV-Service successfully started")

def analyze_frame(frame_id, image_path):
    print(f"Analyzing frame {frame_id}. File {image_path}")

    if not os.path.exists(image_path):
        print(f"File {image_path} not found")
        return None
    
    results = model(image_path)

    kamaz_defects = ["CRACK", "GEOMETRY_DISTORTION", "WELD_DEFECT", "MISSING_HOLE"]
    
    defect_probability = 0.3
    
    if random.random() < defect_probability:
        has_defect = True
        defect_type = random.choice(kamaz_defects)
        max_confidence = random.uniform(0.75, 0.98)
    else:
        has_defect = False
        defect_type = "NONE"
        max_confidence = 1.0

    response_payload = {
        "frameId": frame_id,
        "hasDefect": has_defect,
        "defectType": defect_type,
        "confidence": round(max_confidence, 3)
    }

    producer.send(
        TOPIC_NAME,
        key=frame_id.encode('utf-8'), 
        value=response_payload)
    print(f"Successfully sent to kafka topic {TOPIC_NAME} response {response_payload}")
    
    return {
        "status": "FAILED" if has_defect else "PASSED",
        "defect_type": defect_type,
        "confidence": round(max_confidence, 3)
    }

if __name__ == "__main__":
    print("=== [CV-Service] Conveyor simulation: START  ===")
    
    test_images = ["images/frame_good.png", "images/frame_bad.jpg"]
    frame_counter = 1

    try:
        while True:
            frame_id = f"KAMAZ-FRAME-{2026:04d}-{frame_counter:05d}"
            
            selected_img = random.choice(test_images)
            
            analyze_frame(frame_id, selected_img)
            
            frame_counter += 1
            
            print("Waiting for the next frame off the assembly line...\n")
            time.sleep(5.0) 
            
    except KeyboardInterrupt:
        print("=== Conveyor simulation stopped manually ===")
