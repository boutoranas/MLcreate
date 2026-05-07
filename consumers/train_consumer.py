"""SQS consumer: polls 'preprocessing_done' queue and triggers train handler."""
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
    queue_name = os.environ.get('SQS_QUEUE_PREPROCESSING_DONE', 'cloudml-preprocessing-done')
    queue_url = None
    while queue_url is None:
        try:
            queue_url = sqs_utils.get_queue_url(queue_name)
            print(f"[Train] Connected to SQS queue: {queue_url}")
        except Exception as exc:
            print(f"[Train] SQS not ready: {exc}")
            time.sleep(5)

    print(f"[Train] Polling queue {queue_name}")
    while True:
        try:
            messages = sqs_utils.receive_messages(queue_url, max_messages=1, wait_seconds=20)
            for sqs_msg in messages:
                receipt = sqs_msg['ReceiptHandle']
                try:
                    msg = json.loads(sqs_msg['Body'])
                    job_id = msg.get('job_id')
                    processed_path = msg.get('processed_path')

                    print(f"\n[Train] Received job {job_id}, processed_path={processed_path}")

                    out_dir = os.path.join(os.getcwd(), 'messages')
                    os.makedirs(out_dir, exist_ok=True)
                    msg_file = os.path.join(out_dir, f"preprocess_{job_id}.json")
                    with open(msg_file, 'w') as f:
                        json.dump(msg, f, indent=2)

                    handler_path = os.path.join(os.getcwd(), 'functions', 'train', 'handler.py')
                    print(f"[Train] Running: python {handler_path} {msg_file}")
                    subprocess.check_call(['python', handler_path, msg_file])

                    sqs_utils.delete_message(queue_url, receipt)
                    print(f"[Train] ✓ Job {job_id} completed")
                except Exception:
                    print("[Train] Error processing message:")
                    traceback.print_exc()
        except Exception:
            print("[Train] Error polling queue:")
            traceback.print_exc()
            time.sleep(5)


if __name__ == '__main__':
    main()
