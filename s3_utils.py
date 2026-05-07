import os

try:
    import boto3
    from botocore.exceptions import ClientError
    _BOTO3_AVAILABLE = True
except ImportError:
    _BOTO3_AVAILABLE = False


def _client():
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.environ.get('AWS_SESSION_TOKEN'),
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    )


def _bucket():
    return os.environ.get('S3_BUCKET')


def enabled():
    return _BOTO3_AVAILABLE and bool(_bucket())


def upload_file(local_path, s3_key):
    if not enabled():
        return None
    bucket = _bucket()
    _client().upload_file(local_path, bucket, s3_key)
    url = f"s3://{bucket}/{s3_key}"
    print(f"[S3] Uploaded {local_path} → {url}")
    return url


def download_file(s3_key, local_path):
    if not enabled():
        return False
    os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
    try:
        _client().download_file(_bucket(), s3_key, local_path)
        print(f"[S3] Downloaded s3://{_bucket()}/{s3_key} → {local_path}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
            return False
        raise


def upload_directory(local_dir, s3_prefix):
    """Upload a local directory recursively (used for Spark model dirs)."""
    if not enabled():
        return None
    bucket = _bucket()
    s3 = _client()
    for root, _, files in os.walk(local_dir):
        for fname in files:
            local_file = os.path.join(root, fname)
            rel = os.path.relpath(local_file, local_dir).replace('\\', '/')
            s3.upload_file(local_file, bucket, f"{s3_prefix}/{rel}")
    url = f"s3://{bucket}/{s3_prefix}"
    print(f"[S3] Uploaded directory {local_dir} → {url}")
    return url


def download_directory(s3_prefix, local_dir):
    """Download a directory from S3 (used for Spark model dirs). Returns True if anything was downloaded."""
    if not enabled():
        return False
    bucket = _bucket()
    s3 = _client()
    paginator = s3.get_paginator('list_objects_v2')
    found = False
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix + '/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            rel = os.path.relpath(key, s3_prefix).replace('/', os.sep)
            local_file = os.path.join(local_dir, rel)
            os.makedirs(os.path.dirname(os.path.abspath(local_file)), exist_ok=True)
            s3.download_file(bucket, key, local_file)
            found = True
    if found:
        print(f"[S3] Downloaded s3://{bucket}/{s3_prefix} → {local_dir}")
    return found
