import json, uuid, hashlib, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
from azure.storage.blob import BlobServiceClient, ContentSettings


AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = "artifacts"

# Initialize blob service
blob_service = None
if AZURE_CONN_STR:
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    try:
        blob_service.create_container(AZURE_CONTAINER)
    except Exception:
        pass  


def upload_to_blob(run_id: str, filename: str, data: bytes | str | Dict[str, Any]):
    """Upload a file, string, or dict to Azure Blob Storage with correct content type."""
    if blob_service is None:
        print("No Azure Blob connection. Skipping upload.")
        return

    blob_client = blob_service.get_blob_client(
        container=AZURE_CONTAINER,
        blob=f"runs/{run_id}/{filename}"
    )

    
    if filename.endswith(".json"):
        content_type = "application/json"
    elif filename.endswith(".txt"):
        content_type = "text/plain"
    elif filename.endswith(".md"):
        content_type = "text/markdown"
    elif filename.endswith(".joblib"):
        content_type = "application/octet-stream" 
    else:
        content_type = "application/octet-stream"

    
    if isinstance(data, dict):
        data = json.dumps(data, indent=2)

    
    if isinstance(data, str):
        data = data.encode("utf-8")

   
    blob_client.upload_blob(
        data,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type)
    )
    print(f"✅ Uploaded {filename} to Azure Blob with type {content_type}")


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
