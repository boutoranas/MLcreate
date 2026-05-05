"""Ingest handler (OpenFaaS-style) - CLI helper for local testing.

Usage (local): python handler.py <csv_path>
Environment: KAFKA_BOOTSTRAP for kafka address
"""
import os
import sys
import json
import uuid
from datetime import datetime

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


def publish_message(message, topic="dataset_uploaded"):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    out_dir = os.path.join(os.getcwd(), "messages")
    os.makedirs(out_dir, exist_ok=True)
    
    # Always write to disk for local dev/testing
    msg_file = os.path.join(out_dir, f"dataset_uploaded_{message['job_id']}.json")
    with open(msg_file, "w") as f:
        json.dump(message, f, indent=2)
    print(f"Wrote message to {msg_file}")
    
    if KafkaProducer is None:
        return
    
    try:
        p = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        p.send(topic, message)
        p.flush()
        print("Published message to Kafka topic", topic)
    except Exception as exc:
        print(f"Kafka unavailable ({exc}); using local fallback only")


def main():
    if len(sys.argv) < 2:
        print("Usage: python handler.py <csv_path>")
        sys.exit(1)
    csv_path = sys.argv[1]
    # Convert to absolute path for unambiguous resolution
    csv_path = os.path.abspath(csv_path)
    msg = {
        "job_id": str(uuid.uuid4()),
        "uploader": os.environ.get("USER", "local"),
        "csv_path": csv_path,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    print(f"Job ID: {msg['job_id']}")
    publish_message(msg)


if __name__ == "__main__":
    main()
