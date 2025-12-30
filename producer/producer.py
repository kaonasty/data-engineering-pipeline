import json
import time
import requests
from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9092"
TOPIC = "users_stream"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    try:
        response = requests.get("https://randomuser.me/api/").json()
        user = response["results"][0]
        
        producer.send(TOPIC, user)
        print("Sent user:", user["email"], flush=True)

        time.sleep(2)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5)
