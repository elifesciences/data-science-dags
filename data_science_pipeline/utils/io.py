from io import BytesIO
from pathlib import Path
from typing import Any

import joblib

from s3path import S3Path


def get_path(path: str) -> Path:
    if path.startswith('s3://'):
        return S3Path.from_uri(path)
    return Path(path)


def write_bytes(path: str, data: bytes):
    get_path(path).write_bytes(data)


def write_text(path: str, text: str):
    get_path(path).write_text(text)


def serialize_object_to(value: Any, path: str):
    # it seems to be much faster to write the bytes in one go
    get_path(path).parent.mkdir(parents=True, exist_ok=True)
    buffer = BytesIO()
    joblib.dump(value, buffer)
    write_bytes(path, buffer.getvalue())
