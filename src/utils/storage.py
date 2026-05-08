"""MinIO / S3-compatible object store wrapper."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError

from src.utils.config import get_settings

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_s3_client():
    cfg = get_settings().storage
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
    )


def ensure_bucket() -> None:
    """Create bucket if it does not exist."""
    cfg = get_settings().storage
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=cfg.bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            s3.create_bucket(Bucket=cfg.bucket)
            log.info("Created MinIO bucket: %s", cfg.bucket)
        else:
            raise


def put_json(prefix: str, key: str, data: Any) -> str:
    """Serialise data as JSON and upload. Returns full S3 key."""
    cfg = get_settings().storage
    s3_key = f"{prefix}{key}"
    payload = json.dumps(data, default=str).encode("utf-8")
    get_s3_client().put_object(
        Bucket=cfg.bucket,
        Key=s3_key,
        Body=payload,
        ContentType="application/json",
    )
    log.debug("Stored %d bytes → s3://%s/%s", len(payload), cfg.bucket, s3_key)
    return s3_key


def get_json(prefix: str, key: str) -> Any:
    """Download and deserialise JSON object."""
    cfg = get_settings().storage
    s3_key = f"{prefix}{key}"
    resp = get_s3_client().get_object(Bucket=cfg.bucket, Key=s3_key)
    return json.loads(resp["Body"].read())


def put_bytes(
    prefix: str,
    key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> str:
    cfg = get_settings().storage
    s3_key = f"{prefix}{key}"
    get_s3_client().put_object(
        Bucket=cfg.bucket, Key=s3_key, Body=data, ContentType=content_type
    )
    return s3_key


def get_bytes(prefix: str, key: str) -> bytes:
    cfg = get_settings().storage
    s3_key = f"{prefix}{key}"
    resp = get_s3_client().get_object(Bucket=cfg.bucket, Key=s3_key)
    return resp["Body"].read()


def list_keys(prefix: str) -> list[str]:
    cfg = get_settings().storage
    paginator = get_s3_client().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=cfg.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def raw_key_for(connector_id: str, ts: datetime | None = None) -> str:
    ts = ts or datetime.now(timezone.utc)
    return f"{ts.strftime('%Y/%m/%d/%H')}/{connector_id}.json"
