"""Ingest handler (OpenFaaS-style) - CLI helper for local testing.

Usage (local): python handler.py <csv_path> [job_id] [task_type]
Environment: KAFKA_BOOTSTRAP for kafka address
"""
import os
import sys
import json
import uuid
from datetime import datetime

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

try:
    import sqs_utils
except ImportError:
    sqs_utils = None

try:
    import s3_utils
except ImportError:
    s3_utils = None


def publish_message(message, queue_env="SQS_QUEUE_DATASET_UPLOADED"):
    out_dir = os.path.join(os.getcwd(), "messages")
    os.makedirs(out_dir, exist_ok=True)
    msg_file = os.path.join(out_dir, f"dataset_uploaded_{message['job_id']}.json")
    with open(msg_file, "w") as f:
        json.dump(message, f, indent=2)
    print(f"Wrote message to {msg_file}")

    if sqs_utils is None:
        return
    queue = os.environ.get(queue_env, "cloudml-dataset-uploaded")
    try:
        sqs_utils.send_message(queue, message)
    except Exception as exc:
        print(f"SQS unavailable ({exc}); using local fallback only")


def main():
    if len(sys.argv) < 2:
        print("Usage: python handler.py <csv_path> [job_id] [task_type]")
        sys.exit(1)
    csv_path = sys.argv[1]
    incoming_job_id = sys.argv[2] if len(sys.argv) > 2 else None
    task_type = (sys.argv[3] if len(sys.argv) > 3 else os.environ.get("TASK_TYPE", "classification")).lower()
    s3_csv_key = sys.argv[4] if len(sys.argv) > 4 else None
    # Convert to absolute path for unambiguous resolution
    csv_path = os.path.abspath(csv_path)

    if not os.path.exists(csv_path) and s3_csv_key and s3_utils and s3_utils.enabled():
        print(f"[Ingest] {csv_path} not found locally; downloading from S3 key {s3_csv_key}...")
        s3_utils.download_file(s3_csv_key, csv_path)
    job_id = incoming_job_id or str(uuid.uuid4())

    # Try to collect a small preview and column info for the UI.
    columns = []
    preview = []
    row_count = 0
    raw_preview = f"Job queued: {job_id}"

    try:
        import csv

        with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(8192)
            f.seek(0)
            try:
                has_header = csv.Sniffer().has_header(sample)
            except Exception:
                has_header = False

            reader = csv.reader(f)
            rows = []
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                rows.append(row)

        if rows:
            if has_header:
                columns = rows[0]
                preview = rows[1:]
            else:
                columns = []
                preview = rows

        # Count rows (may be expensive for very large files, but useful for UI)
        try:
            with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
                # subtract header row if present
                row_count = sum(1 for _ in f)
                if has_header and row_count > 0:
                    row_count -= 1
        except Exception:
            row_count = 0

    except Exception:
        # Keep defaults on any parsing error
        columns = []
        preview = []
        row_count = 0

    msg = {
        "job_id": job_id,
        "uploader": os.environ.get("USER", "local"),
        "csv_path": csv_path,
        "task_type": task_type,
        "model_type": task_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "preview": preview,
        "raw_preview": raw_preview,
    }
    print(f"Job ID: {msg['job_id']}")
    publish_message(msg)


if __name__ == "__main__":
    main()
