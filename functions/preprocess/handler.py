"""Preprocess handler: consumes dataset_uploaded message, runs Spark job, writes processed data path.

For local dev the handler can be invoked as: python handler.py <message_file.json>
"""
import os
import sys
import json
from datetime import datetime
import subprocess

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
try:
    import s3_utils
except ImportError:
    s3_utils = None

try:
    import sqs_utils
except ImportError:
    sqs_utils = None


def run_spark(csv_path, out_path):
    cmd = ["python", os.path.join(os.path.dirname(__file__), "..", "..", "spark", "spark_job.py"), csv_path, out_path]
    subprocess.check_call(cmd)


def publish_message(message, queue_env="SQS_QUEUE_PREPROCESSING_DONE"):
    out_dir = os.path.join(os.getcwd(), "messages")
    os.makedirs(out_dir, exist_ok=True)
    msg_file = os.path.join(out_dir, f"preprocess_{message['job_id']}.json")
    with open(msg_file, "w") as f:
        json.dump(message, f, indent=2)
    print(f"Wrote message to {msg_file}")

    if sqs_utils is None:
        return
    queue = os.environ.get(queue_env, "cloudml-preprocessing-done")
    try:
        sqs_utils.send_message(queue, message)
    except Exception as exc:
        print(f"SQS unavailable ({exc}); using local fallback only")


def read_parquet_metadata(out_path):
    ignore_cols = {"label", "target", "prediction", "id"}
    try:
        import pandas as pd

        df_meta = pd.read_parquet(out_path)
        n_rows = int(len(df_meta))
        features = [c for c in columns if c not in ignore_cols]
        return n_rows, features
    except Exception:
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(out_path)
            columns = table.column_names
            n_rows = int(table.num_rows)
            features = [c for c in columns if c not in ignore_cols]
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
    s3_csv_key = msg.get("s3_csv_key")
    job_id = msg.get("job_id")
    task_type = (msg.get("task_type") or msg.get("model_type") or "classification").lower()
    processed_dir = os.environ.get("PROCESSED_DIR", os.path.join(os.getcwd(), "processed"))
    os.makedirs(processed_dir, exist_ok=True)
    out_path = os.path.join(processed_dir, f"{job_id}.parquet")

    csv_path = os.path.abspath(csv_path) if csv_path else csv_path
    if csv_path and not os.path.exists(csv_path) and s3_utils and s3_utils.enabled():
        if s3_csv_key:
            key = s3_csv_key
        else:
            import re
            # Local filename has a timestamp prefix (e.g. "1778177220241_file.csv")
            # but S3 stores the original filename without it
            basename = re.sub(r'^\d+_', '', os.path.basename(csv_path))
            key = f"uploads/{job_id}/{basename}"
        print(f"[Preprocess] {csv_path} not found locally; downloading from S3 key {key}...")
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        s3_utils.download_file(key, csv_path)

    run_spark(csv_path, out_path)
    n_rows, features = read_parquet_metadata(out_path)

    s3_processed_path = None
    if s3_utils and s3_utils.enabled():
        if os.path.isdir(out_path):
            s3_processed_path = s3_utils.upload_directory(out_path, f"processed/{job_id}.parquet")
        else:
            s3_processed_path = s3_utils.upload_file(out_path, f"processed/{job_id}.parquet")

    done_msg = {
        "job_id": job_id,
        "processed_path": out_path,
        "s3_processed_path": s3_processed_path,
        "n_rows": n_rows,
        "features": features,
        "task_type": task_type,
        "model_type": task_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    publish_message(done_msg)


if __name__ == "__main__":
    main()
