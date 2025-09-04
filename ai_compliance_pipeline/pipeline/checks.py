from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, Any, List
from pathlib import Path
import json

@dataclass
class Finding:
    id: str
    title: str
    severity: str   # "BLOCKER", "WARN", "INFO"
    passed: bool
    details: str

def run_checks(meta: Dict[str, Any], run_dir: Path) -> List[Finding]:
    """Run basic compliance checks against metadata."""
    findings: List[Finding] = []

    # 1. Run ID
    findings.append(Finding(
        id="CHECK-001",
        title="Run ID present",
        severity="BLOCKER",
        passed=bool(meta.get("run_id")),
        details="Each run must have a unique identifier."
    ))

    # 2. Dataset hash
    dataset = meta.get("dataset", {})
    findings.append(Finding(
        id="CHECK-002",
        title="Dataset fingerprint exists",
        severity="BLOCKER",
        passed=bool(dataset.get("dataset_sha256")),
        details="Dataset SHA-256 must be logged for reproducibility."
    ))

    # 3. Model metrics
    model = meta.get("model", {})
    metric_val = (model.get("metrics") or {}).get("value")
    findings.append(Finding(
        id="CHECK-003",
        title="Model metrics recorded",
        severity="BLOCKER",
        passed=metric_val is not None,
        details="At least one evaluation metric must be saved."
    ))

    # 4. Model artifact
    artifact_path = model.get("artifact_path")
    findings.append(Finding(
        id="CHECK-004",
        title="Model artifact saved",
        severity="WARN",
        passed=artifact_path and Path(artifact_path).exists(),
        details="Trained model should be persisted for auditability."
    ))

    # 5. Global log exists
    global_log = run_dir.parent.parent / "run_log.jsonl"
    findings.append(Finding(
        id="CHECK-005",
        title="Global run log exists",
        severity="WARN",
        passed=global_log.exists(),
        details="Append-only diary of all runs must exist."
    ))

    return findings

def write_findings(run_dir: Path, findings: List[Finding]) -> str:
    """Save compliance results and return PASS/WARN/FAIL status."""
    run_dir.mkdir(parents=True, exist_ok=True)

    # Save detailed findings
    path = run_dir / "compliance_findings.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump([asdict(finding) for finding in findings], f, indent=2)

    # Determine overall status
    blockers = [f for f in findings if f.severity == "BLOCKER" and not f.passed]
    warns = [f for f in findings if f.severity == "WARN" and not f.passed]

    if blockers:
        status = "FAIL"
    elif warns:
        status = "WARN"
    else:
        status = "PASS"

    # Save summary
    summary_path = run_dir / "compliance_summary.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write(status + "\n")
        for fi in findings:
            if not fi.passed:
                f.write(f"- {fi.id} [{fi.severity}]: {fi.title} — {fi.details}\n")

    return status
