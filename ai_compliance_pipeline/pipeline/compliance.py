import json, uuid, hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

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