"""Preprocess handler: consumes dataset_uploaded message, runs Spark job, writes processed data path.

For local dev the handler can be invoked as: python handler.py <message_file.json>
"""
import os
import sys
import json
from datetime import datetime
import subprocess

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


def run_spark(csv_path, out_path):
    # Call the spark job script; in deployed env you would submit to a Spark cluster
    cmd = ["python", os.path.join(os.path.dirname(__file__), "..", "..", "spark", "spark_job.py"), csv_path, out_path]
    subprocess.check_call(cmd)


def publish_message(message, topic="preprocessing_done"):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    out_dir = os.path.join(os.getcwd(), "messages")
    os.makedirs(out_dir, exist_ok=True)
    
    # Always write to disk for local dev/testing
    msg_file = os.path.join(out_dir, f"preprocess_{message['job_id']}.json")
    with open(msg_file, "w") as f:
        json.dump(message, f, indent=2)
    print(f"Wrote message to {msg_file}")
    
    if KafkaProducer is None:
        return
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, message)
        producer.flush()
        print("Published message to Kafka topic", topic)
    except Exception as exc:
        print(f"Kafka unavailable ({exc}); using local fallback only")


def read_parquet_metadata(out_path):
    try:
        import pandas as pd

        df_meta = pd.read_parquet(out_path)
        n_rows = int(len(df_meta))
        if "label" in df_meta.columns:
            features = [c for c in df_meta.columns if c != "label"]
        else:
            features = list(df_meta.columns)
        return n_rows, features
    except Exception:
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(out_path)
            columns = table.column_names
            n_rows = int(table.num_rows)
            if "label" in columns:
                features = [c for c in columns if c != "label"]
            else:
                features = list(columns)
            return n_rows, features
        except Exception:
            return None, None


def main():
    if len(sys.argv) < 2:
        print("Usage: python handler.py <message.json>")
        sys.exit(1)
    msg_file = sys.argv[1]
    with open(msg_file) as f:
        msg = json.load(f)
    csv_path = msg.get("csv_path")
    job_id = msg.get("job_id")
    task_type = (msg.get("task_type") or msg.get("model_type") or "classification").lower()
    processed_dir = os.environ.get("PROCESSED_DIR", os.path.join(os.getcwd(), "processed"))
    os.makedirs(processed_dir, exist_ok=True)
    out_path = os.path.join(processed_dir, f"{job_id}.parquet")
    run_spark(csv_path, out_path)
    n_rows, features = read_parquet_metadata(out_path)

    done_msg = {
        "job_id": job_id,
        "processed_path": out_path,
        "n_rows": n_rows,
        "features": features,
        "task_type": task_type,
        "model_type": task_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    publish_message(done_msg)


if __name__ == "__main__":
    main()
