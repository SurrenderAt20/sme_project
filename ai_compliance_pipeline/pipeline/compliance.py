import json, uuid, hashlib, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
import os
from azure.storage.blob import BlobServiceClient

# Get connection string from environment variable (safer than hardcoding)
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = "artifacts"  # make sure this container exists in your Storage Account

_blob_service = None
if AZURE_CONN_STR:
    _blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    try:
        _blob_service.create_container(AZURE_CONTAINER)
    except Exception:
        pass  # ignore if it already exists

def upload_to_blob(run_id: str, filename: str, data: bytes | str):
    """Upload a file or JSON string to Azure Blob Storage under runs/<run_id>/."""
    if not _blob_service:
        return  # no connection configured → skip silently

    blob_client = _blob_service.get_blob_client(
        container=AZURE_CONTAINER, blob=f"runs/{run_id}/{filename}"
    )

    # Convert dict/str to bytes if needed
    if isinstance(data, str):
        blob_client.upload_blob(data.encode("utf-8"), overwrite=True)
    elif isinstance(data, bytes):
        blob_client.upload_blob(data, overwrite=True)
    else:
        import json
        blob_client.upload_blob(json.dumps(data, indent=2).encode("utf-8"), overwrite=True)


def new_run_id() -> str:
    """Generate a unique ID for this pipeline run."""
    return str(uuid.uuid4())

def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()

def sha256_of_file(path: Path) -> str:
    """Compute SHA-256 hash of a file (like a fingerprint)."""
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def append_jsonl(log_path: Path, obj: Dict[str, Any]) -> None:
    """Append a JSON object as one line to a .jsonl file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(obj, ensure_ascii=False) + '\n')

def write_json(path: Path, obj: Dict[str, Any]) -> None:
    """Write a JSON file with pretty formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)