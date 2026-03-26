"""
MinIO (S3-compatible) client utilities.
Wraps boto3 with helpers for Parquet upload/download.
"""
import io
import os
import logging
from typing import List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

logger = logging.getLogger(__name__)


def get_minio_client():
    """Return a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_parquet(client, bucket: str, key: str, table: pa.Table) -> None:
    """Serialize a PyArrow Table as Parquet and upload to MinIO."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    size = buf.getbuffer().nbytes
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentLength=size,
        ContentType="application/octet-stream",
    )
    logger.info("Uploaded s3://%s/%s  (%d bytes)", bucket, key, size)


def download_parquet(client, bucket: str, key: str) -> pa.Table:
    """Download a Parquet file from MinIO and return as PyArrow Table."""
    response = client.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(response["Body"].read())
    table = pq.read_table(buf)
    logger.info("Downloaded s3://%s/%s  (%d rows)", bucket, key, table.num_rows)
    return table


def list_objects(client, bucket: str, prefix: str = "") -> List[str]:
    """List object keys under a bucket prefix."""
    try:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]
    except Exception as exc:
        logger.error("Error listing s3://%s/%s: %s", bucket, prefix, exc)
        return []


def ensure_buckets(client, *buckets: str) -> None:
    """Create buckets if they do not already exist.

    boto3 raises botocore.exceptions.ClientError (not client.exceptions.NoSuchBucket)
    for missing buckets. We check the HTTP status code to distinguish 404 from other errors.
    """
    from botocore.exceptions import ClientError
    for bucket in buckets:
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                client.create_bucket(Bucket=bucket)
                logger.info("Created bucket: %s", bucket)
            else:
                raise
