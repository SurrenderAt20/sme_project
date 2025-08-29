from __future__ import annotations
import argparse
from pipeline.reports import write_dataset_card, write_model_card, write_run_report
from pathlib import Path
from typing import Dict, Any
from rich import print
from pipeline.ingestion import load_csv, dataframe_schema
from pipeline.compliance import new_run_id, utc_now_iso, sha256_of_file, append_jsonl, write_json
from pipeline.transform import basic_clean, prepare_features, train_test_split_simple
from pipeline.model import train_logreg, evaluate, save_model

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
    ap = argparse.ArgumentParser(description="Day 1: end-to-end MVP (transform + model + logs)")
    ap.add_argument("--data", type=str, default="data/bank.csv", help="Path to CSV dataset")
    args = ap.parse_args()

    paths = ensure_dirs()
    dataset_path = Path(args.data)
    if not dataset_path.exists():
        print(f"[red]Dataset not found:[/red] {dataset_path}")
        raise SystemExit(1)

    print("[bold cyan]Step 1: Load dataset[/bold cyan]")
    df = load_csv(dataset_path)
    print(f"[green]Loaded[/green] {len(df)} rows, {len(df.columns)} columns.")

    print("[bold cyan]Step 2: Create run + base metadata[/bold cyan]")
    run_id = new_run_id()
    run_dir = paths["runs"] / run_id
    base = {"run_id": run_id, "timestamp_utc": utc_now_iso(), "dataset": gather_metadata(dataset_path, df)}

    print("[bold cyan]Step 3: Transform[/bold cyan]")
    df_clean = basic_clean(df)
    X, y = prepare_features(df_clean)
    X_train, X_test, y_train, y_test = train_test_split_simple(X, y)
    transform_meta = {
        "rows_after_clean": int(len(df_clean)),
        "feature_count": int(X.shape[1]),
        "train_size": int(len(y_train)),
        "test_size": int(len(y_test)),
    }

    print("[bold cyan]Step 4: Train + evaluate[/bold cyan]")
    model = train_logreg(X_train, y_train, max_iter=1000)
    metrics = evaluate(model, X_test, y_test)
    model_path = save_model(model, run_dir / "model.joblib")
    model_meta = {
        "algorithm": "LogisticRegression",
        "hyperparameters": {"max_iter": 1000},
        "artifact_path": model_path,
        "metrics": metrics,
    }

    record = {**base, "transform": transform_meta, "model": model_meta}
    append_jsonl(paths["log"], record)
    write_json(run_dir / "metadata.json", record)
    write_dataset_card(run_dir, record)
    write_model_card(run_dir, record)
    write_run_report(run_dir, record)


    print("[bold cyan]Done![/bold cyan]")
    print(f"• Run ID: [bold]{run_id}[/bold]")
    print(f"• Accuracy: {metrics['value']:.3f}")
    print(f"• Global log: {paths['log']}")
    print(f"• Per-run metadata: {run_dir/'metadata.json'}")
    print(f"• Model: {model_path}")

if __name__ == "__main__":
    main()
