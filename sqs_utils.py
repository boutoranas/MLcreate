import os
import json
import boto3


def _client():
    return boto3.client(
        'sqs',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.environ.get('AWS_SESSION_TOKEN'),
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )


def enabled():
    return bool(os.environ.get('AWS_ACCESS_KEY_ID') or os.environ.get('AWS_DEFAULT_REGION'))


def get_queue_url(queue_name_or_url):
    if queue_name_or_url.startswith('https://'):
        return queue_name_or_url
    return _client().get_queue_url(QueueName=queue_name_or_url)['QueueUrl']


def send_message(queue_name_or_url, message_dict):
    url = get_queue_url(queue_name_or_url)
    _client().send_message(QueueUrl=url, MessageBody=json.dumps(message_dict))
    print(f"[SQS] Sent message to {url}")


def receive_messages(queue_url, max_messages=1, wait_seconds=20):
    response = _client().receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_seconds,
    )
    return response.get('Messages', [])


def delete_message(queue_url, receipt_handle):
    _client().delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
