"""Kafka consumer: listens to 'predict_requested' topic and triggers predict handler."""
import os
import json
import subprocess
import time
import traceback
import re
from kafka import KafkaConsumer


def fix_model_metadata_nan():
    """Fix NaN values in all Spark model metadata files."""
    models_dir = os.environ.get('MODELS_DIR', '/workspace/models')
    if not os.path.isdir(models_dir):
        print(f"[Predict] Models directory not found: {models_dir}")
        return
    
    fixed_count = 0
    for root, dirs, files in os.walk(models_dir):
        for file in files:
            if file == 'part-00000' and 'metadata' in root:
                filepath = os.path.join(root, file)
                try:
                    with open(filepath, 'r') as f:
                        content = f.read()
                    if ':NaN' in content or ':Infinity' in content or ':-Infinity' in content:
                        fixed_content = re.sub(r':NaN\b', ':null', content)
                        fixed_content = re.sub(r':Infinity\b', ':null', fixed_content)
                        fixed_content = re.sub(r':-Infinity\b', ':null', fixed_content)
                        with open(filepath, 'w') as f:
                            f.write(fixed_content)
                        print(f"[Predict] Fixed NaN in metadata: {filepath}")
                        fixed_count += 1
                except Exception as e:
                    print(f"[Predict] Warning: Could not fix metadata {filepath}: {e}")
    
    if fixed_count > 0:
        print(f"[Predict] Fixed {fixed_count} metadata files")


def wait_for_consumer(bootstrap: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            print(f"[Predict] Connecting to Kafka at {bootstrap}, topic: {topic}")
            return KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="predict_group",
            )
        except Exception as exc:
            print(f"[Predict] Kafka not ready yet: {exc}")
            time.sleep(5)


def main():
    # Fix model metadata files first
    fix_model_metadata_nan()
    
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = "predict_requested"
    consumer = wait_for_consumer(bootstrap, topic)

    print(f"[Predict] Listening on topic {topic}")
    for msg_record in consumer:
        try:
            msg = msg_record.value
            predict_id = msg.get("predict_id")
            csv_path = msg.get("csv_path")
            model_id = msg.get("model_id")
            model_type = msg.get("model_type", "classification")

            print(f"\n[Predict] Received predict_id {predict_id}, model_id={model_id}, type={model_type}")
            print(f"[Predict] CSV path: {csv_path}")

            # Write message to disk for handler to read
            out_dir = os.path.join(os.getcwd(), "messages")
            os.makedirs(out_dir, exist_ok=True)
            msg_file = os.path.join(out_dir, f"predict_request_{predict_id}.json")
            with open(msg_file, "w") as f:
                json.dump(msg, f, indent=2)

            handler_path = os.path.join(os.getcwd(), "functions", "predict", "handler.py")
            print(f"[Predict] Running: python {handler_path} {csv_path} {model_id} {model_type}")
            subprocess.check_call(["python", handler_path, csv_path, model_id, model_type])
            print(f"[Predict] ✓ Predict job {predict_id} completed")
        except Exception:
            print("[Predict] Error while processing message:")
            traceback.print_exc()


if __name__ == "__main__":
    main()
