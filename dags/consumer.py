import json
import os
import time
from datetime import datetime

from kafka import KafkaConsumer
import boto3
from dotenv import load_dotenv
from airflow.decorators import dag, task

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

KAFKA_BOOTSTRAP = "kafka:9092"

# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="kafka_to_s3_fixed_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def kafka_to_s3_fixed_dag():

    @task()
    def consume_and_upload():

        # -----------------------------
        # KAFKA CONSUMER
        # -----------------------------
        consumer = KafkaConsumer(
            "products",
            "users",
            "carts",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=None  # 🔥 no offset tracking (debug mode)
        )

        # -----------------------------
        # S3 CLIENT
        # -----------------------------
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )

        topic_to_folder = {
            "products": "raw_products",
            "users": "raw_users",
            "carts": "raw_carts"
        }

        print("Waiting for Kafka partition assignment...")

        # Wait for partitions
        while not consumer.assignment():
            consumer.poll(1000)

        print("Assigned partitions:", consumer.assignment())

        # 🔥 CRITICAL FIX
        consumer.seek_to_beginning()

        print("Reading data from Kafka...")

        # -----------------------------
        # FETCH DATA
        # -----------------------------
        all_data = {}
        start_time = time.time()
        max_wait = 60  # increased for stability

        while time.time() - start_time < max_wait:
            messages = consumer.poll(timeout_ms=2000)

            for tp, records in messages.items():
                topic = tp.topic

                if topic not in all_data:
                    all_data[topic] = []

                for r in records:
                    all_data[topic].append(r.value)

        # -----------------------------
        # CHECK DATA
        # -----------------------------
        if not all_data or all(len(v) == 0 for v in all_data.values()):
            print("❌ No data fetched from Kafka")
            consumer.close()
            return

        # -----------------------------
        # UPLOAD TO S3
        # -----------------------------
        for topic, batch in all_data.items():

            if not batch:
                continue

            folder = topic_to_folder.get(topic, "unknown")

            s3_key = (
                f"{folder}/"
                f"date={datetime.utcnow().date()}/"
                f"{int(datetime.utcnow().timestamp())}.json"
            )

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(batch)
            )

            print(f"✅ Uploaded {len(batch)} records from {topic} → {folder}")

        consumer.close()
        print("🎉 Task completed successfully")

    consume_and_upload()


dag = kafka_to_s3_fixed_dag()