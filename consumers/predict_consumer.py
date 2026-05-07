"""SQS consumer: polls 'predict_requested' queue and triggers predict handler."""
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


def delete_model_crc_files():
    models_dir = os.environ.get('MODELS_DIR', '/workspace/models')
    if not os.path.isdir(models_dir):
        return
    count = 0
    for root, _, files in os.walk(models_dir):
        for f in files:
            if f.startswith('.') and f.endswith('.crc'):
                try:
                    os.remove(os.path.join(root, f))
                    count += 1
                except Exception as e:
                    print(f"[Predict] Warning: Could not delete CRC file: {e}")
    if count > 0:
        print(f"[Predict] Deleted {count} stale CRC files")


def main():
    delete_model_crc_files()

    queue_name = os.environ.get('SQS_QUEUE_PREDICT_REQUESTED', 'cloudml-predict-requested')
    queue_url = None
    while queue_url is None:
        try:
            queue_url = sqs_utils.get_queue_url(queue_name)
            print(f"[Predict] Connected to SQS queue: {queue_url}")
        except Exception as exc:
            print(f"[Predict] SQS not ready: {exc}")
            time.sleep(5)

    print(f"[Predict] Polling queue {queue_name}")
    while True:
        try:
            messages = sqs_utils.receive_messages(queue_url, max_messages=1, wait_seconds=20)
            for sqs_msg in messages:
                receipt = sqs_msg['ReceiptHandle']
                try:
                    msg = json.loads(sqs_msg['Body'])
                    predict_id = msg.get('predict_id')
                    csv_path = msg.get('csv_path')
                    model_id = msg.get('model_id')
                    model_type = msg.get('model_type', 'classification')

                    print(f"\n[Predict] Received predict_id {predict_id}, model_id={model_id}, type={model_type}")
                    print(f"[Predict] CSV path: {csv_path}")

                    out_dir = os.path.join(os.getcwd(), 'messages')
                    os.makedirs(out_dir, exist_ok=True)
                    msg_file = os.path.join(out_dir, f"predict_request_{predict_id}.json")
                    with open(msg_file, 'w') as f:
                        json.dump(msg, f, indent=2)

                    handler_path = os.path.join(os.getcwd(), 'functions', 'predict', 'handler.py')
                    print(f"[Predict] Running: python {handler_path} {csv_path} {model_id} {model_type}")
                    subprocess.check_call(['python', handler_path, csv_path, model_id, model_type])

                    sqs_utils.delete_message(queue_url, receipt)
                    print(f"[Predict] ✓ Predict job {predict_id} completed")
                except Exception:
                    print("[Predict] Error processing message:")
                    traceback.print_exc()
        except Exception:
            print("[Predict] Error polling queue:")
            traceback.print_exc()
            time.sleep(5)


if __name__ == '__main__':
    main()
