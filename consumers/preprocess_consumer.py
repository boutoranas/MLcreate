"""Kafka consumer: listens to 'dataset_uploaded' topic and triggers preprocess handler."""
import os
import json
import subprocess
import time
import traceback
from kafka import KafkaConsumer


def wait_for_consumer(bootstrap: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            print(f"[Preprocess] Connecting to Kafka at {bootstrap}, topic: {topic}")
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="preprocess_group",
            )
        except Exception as exc:
            print(f"[Preprocess] Kafka not ready yet: {exc}")
            time.sleep(5)


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = "dataset_uploaded"
    consumer = wait_for_consumer(bootstrap, topic)

    print(f"[Preprocess] Listening on topic {topic}")
    for msg_record in consumer:
        try:
            msg = msg_record.value
            job_id = msg.get("job_id")
            csv_path = msg.get("csv_path")

            print(f"\n[Preprocess] Received job {job_id}, csv_path={csv_path}")

            # Write message to disk for handler to read
            out_dir = os.path.join(os.getcwd(), "messages")
            os.makedirs(out_dir, exist_ok=True)
            msg_file = os.path.join(out_dir, f"dataset_uploaded_{job_id}.json")
            with open(msg_file, "w") as f:
                json.dump(msg, f, indent=2)

            handler_path = os.path.join(os.getcwd(), "functions", "preprocess", "handler.py")
            print(f"[Preprocess] Running: python {handler_path} {msg_file}")
            subprocess.check_call(["python", handler_path, msg_file])
            print(f"[Preprocess] ✓ Job {job_id} completed")
        except Exception:
            print("[Preprocess] Error while processing message:")
            traceback.print_exc()


if __name__ == "__main__":
    main()
