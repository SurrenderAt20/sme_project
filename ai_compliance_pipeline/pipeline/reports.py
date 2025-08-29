from pathlib import Path
from typing import Dict, Any

def write_dataset_card(run_dir: Path, meta: Dict[str, Any]):
    """Write a simple dataset card as Markdown."""
    dataset = meta.get("dataset", {})
    lines = [
        "# Dataset Card",
        f"**Path**: {dataset.get('dataset_path')}",
        f"**Rows**: {dataset.get('rows')}",
        f"**Columns**: {len(dataset.get('columns', []))}",
        f"**Schema**: {dataset.get('schema')}",
        f"**SHA-256**: {dataset.get('dataset_sha256')}",
    ]
    path = run_dir / "dataset_card.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)

def write_model_card(run_dir: Path, meta: Dict[str, Any]):
    """Write a simple model card as Markdown."""
    model = meta.get("model", {})
    lines = [
        "# Model Card",
        f"**Algorithm**: {model.get('algorithm')}",
        f"**Hyperparameters**: {model.get('hyperparameters')}",
        f"**Metrics**: {model.get('metrics')}",
        f"**Artifact Path**: {model.get('artifact_path')}",
    ]
    path = run_dir / "model_card.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)

def write_run_report(run_dir: Path, meta: Dict[str, Any]):
    """Write a combined run report as Markdown."""
    lines = [
        "# Run Report",
        f"**Run ID**: {meta.get('run_id')}",
        f"**Timestamp**: {meta.get('timestamp_utc')}",
        "",
        "## Dataset",
        f"- Rows: {meta.get('dataset', {}).get('rows')}",
        f"- Columns: {len(meta.get('dataset', {}).get('columns', []))}",
        f"- Hash: {meta.get('dataset', {}).get('dataset_sha256')}",
        "",
        "## Model",
        f"- Algorithm: {meta.get('model', {}).get('algorithm')}",
        f"- Accuracy: {meta.get('model', {}).get('metrics', {}).get('value')}",
        f"- Saved at: {meta.get('model', {}).get('artifact_path')}",
    ]
    path = run_dir / "run_report.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)
