"""SQS consumer: polls 'csv_upload_requested' queue and triggers ingest handler."""
import os
import json
import subprocess
import time
import traceback
import sys

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import sqs_utils


def main():
    queue_name = os.environ.get('SQS_QUEUE_CSV_UPLOAD_REQUESTED', 'cloudml-csv-upload-requested')
    queue_url = None
    while queue_url is None:
        try:
            queue_url = sqs_utils.get_queue_url(queue_name)
            print(f"[Ingest] Connected to SQS queue: {queue_url}")
        except Exception as exc:
            print(f"[Ingest] SQS not ready: {exc}")
            time.sleep(5)

    print(f"[Ingest] Polling queue {queue_name}")
    while True:
        try:
            messages = sqs_utils.receive_messages(queue_url, max_messages=1, wait_seconds=20)
            for sqs_msg in messages:
                receipt = sqs_msg['ReceiptHandle']
                try:
                    msg = json.loads(sqs_msg['Body'])
                    job_id = msg.get('job_id')
                    csv_path = msg.get('csv_path')
                    task_type = msg.get('task_type') or msg.get('model_type') or 'classification'

                    print(f"\n[Ingest] Received job {job_id}, csv_path={csv_path}, task_type={task_type}")

                    handler_path = os.path.join(os.getcwd(), 'functions', 'ingest', 'handler.py')
                    print(f"[Ingest] Running: python {handler_path} {csv_path} {job_id} {task_type}")
                    subprocess.check_call(['python', handler_path, csv_path, job_id, task_type])

                    sqs_utils.delete_message(queue_url, receipt)
                    print(f"[Ingest] ✓ Job {job_id} completed")
                except Exception:
                    print("[Ingest] Error processing message:")
                    traceback.print_exc()
        except Exception:
            print("[Ingest] Error polling queue:")
            traceback.print_exc()
            time.sleep(5)


if __name__ == '__main__':
    main()
