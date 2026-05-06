"""Kafka consumer: listens to 'csv_upload_requested' topic and triggers ingest handler."""
import os
import json
import subprocess
import time
import traceback
from kafka import KafkaConsumer


def wait_for_consumer(bootstrap: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            print(f"[Ingest] Connecting to Kafka at {bootstrap}, topic: {topic}")
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="ingest_group",
            )
        except Exception as exc:
            print(f"[Ingest] Kafka not ready yet: {exc}")
            time.sleep(5)


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = "csv_upload_requested"
    consumer = wait_for_consumer(bootstrap, topic)

    print(f"[Ingest] Listening on topic {topic}")
    for msg_record in consumer:
        try:
            msg = msg_record.value
            job_id = msg.get("job_id")
            csv_path = msg.get("csv_path")

            print(f"\n[Ingest] Received job {job_id}, csv_path={csv_path}")

            handler_path = os.path.join(os.getcwd(), "functions", "ingest", "handler.py")
            print(f"[Ingest] Running: python {handler_path} {csv_path} {job_id}")
            subprocess.check_call(["python", handler_path, csv_path, job_id])
            print(f"[Ingest] ✓ Job {job_id} completed")
        except Exception:
            print("[Ingest] Error while processing message:")
            traceback.print_exc()


if __name__ == "__main__":
    main()
