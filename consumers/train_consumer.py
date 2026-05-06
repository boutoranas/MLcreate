"""Kafka consumer: listens to 'preprocessing_done' topic and triggers train handler."""
import os
import json
import subprocess
import time
import traceback
from kafka import KafkaConsumer


def wait_for_consumer(bootstrap: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            print(f"[Train] Connecting to Kafka at {bootstrap}, topic: {topic}")
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="train_group",
            )
        except Exception as exc:
            print(f"[Train] Kafka not ready yet: {exc}")
            time.sleep(5)


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = "preprocessing_done"
    consumer = wait_for_consumer(bootstrap, topic)

    print(f"[Train] Listening on topic {topic}")
    for msg_record in consumer:
        try:
            msg = msg_record.value
            job_id = msg.get("job_id")
            processed_path = msg.get("processed_path")

            print(f"\n[Train] Received job {job_id}, processed_path={processed_path}")

            # Write message to disk for handler to read
            out_dir = os.path.join(os.getcwd(), "messages")
            os.makedirs(out_dir, exist_ok=True)
            msg_file = os.path.join(out_dir, f"preprocess_{job_id}.json")
            with open(msg_file, "w") as f:
                json.dump(msg, f, indent=2)

            handler_path = os.path.join(os.getcwd(), "functions", "train", "handler.py")
            print(f"[Train] Running: python {handler_path} {msg_file}")
            subprocess.check_call(["python", handler_path, msg_file])
            print(f"[Train] ✓ Job {job_id} completed")
        except Exception:
            print("[Train] Error while processing message:")
            traceback.print_exc()


if __name__ == "__main__":
    main()
