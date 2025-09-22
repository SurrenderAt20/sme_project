from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, Any, List
from pathlib import Path
import json
from datetime import datetime
import pytz


@dataclass
class Finding:
    id: str
    title: str
    severity: str  # "BLOCKER", "WARN", "INFO"
    passed: bool
    details: str


def run_checks(meta: Dict[str, Any], run_dir: Path) -> List[Finding]:
    """Run basic compliance checks against metadata."""
    findings: List[Finding] = []

    # 1. Run ID
    findings.append(
        Finding(
            id="CHECK-001",
            title="Run ID present",
            severity="BLOCKER",
            passed=bool(meta.get("run_id")),
            details="Each run must have a unique identifier.",
        )
    )

    # 2. Dataset hash
    dataset = meta.get("dataset", {})
    findings.append(
        Finding(
            id="CHECK-002",
            title="Dataset fingerprint exists",
            severity="BLOCKER",
            passed=bool(dataset.get("dataset_sha256")),
            details="Dataset SHA-256 must be logged for reproducibility.",
        )
    )

    # 3. Model metrics
    model = meta.get("model", {})
    metric_val = (model.get("metrics") or {}).get("value")
    findings.append(
        Finding(
            id="CHECK-003",
            title="Model metrics recorded",
            severity="BLOCKER",
            passed=metric_val is not None,
            details="At least one evaluation metric must be saved.",
        )
    )

    # 4. Model artifact
    artifact_path = model.get("artifact_path")
    findings.append(
        Finding(
            id="CHECK-004",
            title="Model artifact saved",
            severity="WARN",
            passed=artifact_path and Path(artifact_path).exists(),
            details="All artefacts (model, dataset card, model card, run report) must be saved for auditability and reproducibility.",
        )
    )

    # 5. Global log exists
    global_log = run_dir.parent.parent / "run_log.jsonl"
    findings.append(
        Finding(
            id="CHECK-005",
            title="Global run log exists",
            severity="WARN",
            passed=global_log.exists(),
            details="Append-only diary of all runs must exist.",
        )
    )

    # 6. Transformation metadata
    transform = meta.get("transform")
    rows_cleaned = transform.get("rows_after_clean") if transform else None
    findings.append(
        Finding(
            id="CHECK-006",
            title="Transformation metadata exists and data cleaned",
            severity="WARN",
            passed=transform is not None and isinstance(rows_cleaned, int) and rows_cleaned > 0,
            details="Transformation step metadata (rows_after_clean, features, splits) must be present and data must be cleaned for reproducibility.",
        )
    )

        # 7. Model card integrity
    model_card_path = run_dir / "model_card.md"
    expected_hash = meta.get("model_card_hash")
    actual_hash = None
    if model_card_path.exists():
        try:
            from pipeline.compliance import sha256_of_file
            actual_hash = sha256_of_file(model_card_path)
        except Exception:
            actual_hash = None
    findings.append(
        Finding(
            id="CHECK-007",
            title="Model card integrity",
            severity="BLOCKER",
            passed=(expected_hash is not None and actual_hash == expected_hash),
            details="Model card hash must match the value saved at creation. Tampering will trigger a FAIL verdict.",
        )
    )

    return findings


def write_findings(run_dir: Path, findings: List[Finding]) -> str:
    """Save compliance results and return PASS/WARN/FAIL status."""
    run_dir.mkdir(parents=True, exist_ok=True)

    path = run_dir / "compliance_findings.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump([asdict(finding) for finding in findings], f, indent=2)

    blockers = [f for f in findings if f.severity == "BLOCKER" and not f.passed]
    warns = [f for f in findings if f.severity == "WARN" and not f.passed]

    if blockers:
        status = "FAIL"
    elif warns:
        status = "WARN"
    else:
        status = "PASS"

    summary_path = run_dir / "compliance_summary.txt"
    run_id = None
    metadata_path = run_dir / "metadata.json"
    if metadata_path.exists():
        try:
            import json as _json
            with metadata_path.open("r", encoding="utf-8") as mf:
                meta = _json.load(mf)
            run_id = meta.get("run_id")
        except Exception:
            pass
    tz = pytz.timezone('Europe/Copenhagen')
    now = datetime.now(tz)
    timestamp = now.strftime('%Y-%m-%d  %H:%M:%S %Z')
    findings_sorted = sorted(findings, key=lambda f: f.id)
    with summary_path.open("w", encoding="utf-8") as f:
        if run_id:
            f.write(f"Compliance Summary for Run: {run_id}\n")
        else:
            f.write("Compliance Summary\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"Overall Status: {status}\n\n")
        f.write("Check Results:\n")
        for fi in findings_sorted:
            f.write(f"- {fi.id} [{fi.severity}]: {fi.title} — {'PASS' if fi.passed else 'FAIL'}\n")
            if fi.details:
                f.write(f"  Details: {fi.details}\n")
    return status
