from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, Any
from rich import print

from pipeline.ingestion import load_csv, dataframe_schema
from pipeline.compliance import (
    new_run_id, utc_now_iso, sha256_of_file, append_jsonl, write_json
)

def ensure_dirs() -> Dict[str, Path]:
    root = Path(__file__).parent.resolve()
    paths = {
        "root": root,
        "data": root / "data",
        "artifacts": root / "artifacts",
        "runs": root / "artifacts" / "runs",
        "log": root / "artifacts" / "run_log.jsonl",
    }
    for p in [paths["data"], paths["artifacts"], paths["runs"]]:
        p.mkdir(parents=True, exist_ok=True)
    return paths

def gather_metadata(dataset_path: Path, df) -> Dict[str, Any]:
    file_size = dataset_path.stat().st_size if dataset_path.exists() else None
    return {
        "dataset_path": str(dataset_path),
        "dataset_sha256": sha256_of_file(dataset_path),
        "file_size_bytes": file_size,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "schema": dataframe_schema(df),
    }

def main():
    ap = argparse.ArgumentParser(description="Step 1: Ingestion + compliance logging")
    ap.add_argument("--data", type=str, default="data/bank.csv",
                    help="Path to CSV dataset")
    args = ap.parse_args()

    paths = ensure_dirs()
    dataset_path = Path(args.data)
    if not dataset_path.exists():
        print(f"[red]Dataset not found:[/red] {dataset_path}")
        raise SystemExit(1)

    print("[bold cyan]Step 1: Loading dataset...[/bold cyan]")
    df = load_csv(dataset_path)
    print(f"[green]Loaded[/green] {len(df)} rows and {len(df.columns)} columns.")

    print("[bold cyan]Step 2: Creating run ID and metadata...[/bold cyan]")
    run_id = new_run_id()
    meta = {
        "run_id": run_id,
        "timestamp_utc": utc_now_iso(),
        "metadata": gather_metadata(dataset_path, df),
    }

    append_jsonl(paths["log"], meta)
    run_dir = paths["runs"] / run_id
    write_json(run_dir / "metadata.json", meta)

    print("[bold cyan]Done![/bold cyan]")
    print(f"• Run ID: [bold]{run_id}[/bold]")
    print(f"• Global log: {paths['log']}")
    print(f"• Per-run metadata: {run_dir/'metadata.json'}")

if __name__ == "__main__":
    main()