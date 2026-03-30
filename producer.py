import json
import requests
from kafka import KafkaProducer

# Kafka Config
BOOTSTRAP_SERVERS = "host.docker.internal:29092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# API Endpoints
APIS = {
    "products": "https://fakestoreapi.com/products",
    "users": "https://fakestoreapi.com/users",
    "carts": "https://fakestoreapi.com/carts"
}


def fetch_and_send(topic, url):
    response = requests.get(url)
    data = response.json()

    print(f"Sending {len(data)} records to {topic}...")

    for record in data:
        producer.send(topic, value=record)

    producer.flush()
    print(f"Data sent to {topic}")


if __name__ == "__main__":
    for topic, url in APIS.items():
        fetch_and_send(topic, url)