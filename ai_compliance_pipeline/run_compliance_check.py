
import sys
import os
import json
from pathlib import Path
from pipeline.checks import run_checks, write_findings

def get_latest_run_dir(runs_root):
    runs = [d for d in os.listdir(runs_root) if os.path.isdir(os.path.join(runs_root, d))]
    if not runs:
        raise FileNotFoundError("No runs found in artifacts/runs.")
    
    runs.sort(key=lambda d: os.path.getctime(os.path.join(runs_root, d)), reverse=True)
    return runs[0]

def main():
    runs_root = os.path.join(os.path.dirname(__file__), "artifacts", "runs")
    run_id = None
    
    for i, arg in enumerate(sys.argv):
        if arg == "--run_id" and i + 1 < len(sys.argv):
            run_id = sys.argv[i + 1]
            break
    if not run_id:
        print("No run_id provided. Using latest run.")
        run_id = get_latest_run_dir(runs_root)
    run_dir = os.path.join(runs_root, run_id)
    metadata_path = os.path.join(run_dir, "metadata.json")
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"metadata.json not found in {run_dir}")
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    findings = run_checks(metadata, Path(run_dir))
    write_findings(Path(run_dir), findings)

    
    try:
        from pipeline.compliance import upload_to_blob
        compliance_findings_path = Path(run_dir) / "compliance_findings.json"
        compliance_summary_path = Path(run_dir) / "compliance_summary.txt"
        if compliance_findings_path.exists():
            upload_to_blob(run_id, "compliance_findings.json", compliance_findings_path.read_text())
        if compliance_summary_path.exists():
            upload_to_blob(run_id, "compliance_summary.txt", compliance_summary_path.read_text())
        print("Uploaded updated compliance artefacts to Azure Blob Storage.")
    except Exception as e:
        print(f"[WARN] Could not upload to Azure Blob Storage: {e}")

    print(f"Compliance check complete for run {run_id}. Findings written.")

if __name__ == "__main__":
    main()
