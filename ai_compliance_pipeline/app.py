from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import json

app = FastAPI(title="AI Compliance Pipeline Viewer")
base = Path(__file__).parent / "artifacts" / "runs"

@app.get("/", response_class=HTMLResponse)
async def home():
    return '<h2>Go to <a href="/runs">/runs</a> to view pipeline runs</h2>'

@app.get("/runs", response_class=HTMLResponse)
async def list_runs():
    if not base.exists():
        return "<p>No runs found.</p>"

    rows = []
    for run_dir in sorted(base.iterdir(), reverse=True):
        meta_path = run_dir / "metadata.json"
        if not meta_path.exists():
            continue
        meta = json.loads(meta_path.read_text())
        rows.append(f"<tr><td>{meta['run_id']}</td>"
                    f"<td>{meta['timestamp_utc']}</td>"
                    f"<td>{meta.get('model', {}).get('metrics', {}).get('value','-')}</td>"
                    f"<td>{meta.get('compliance', {}).get('status','-')}</td>"
                    f"<td><a href='/runs/{meta['run_id']}'>View</a></td></tr>")
    table = "<table border=1><tr><th>Run ID</th><th>Time</th><th>Accuracy</th><th>Compliance</th><th>Link</th></tr>"
    table += "".join(rows) + "</table>"
    return table

@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(run_id: str):
    run_dir = base / run_id
    if not run_dir.exists():
        return f"<p>No run {run_id} found.</p>"

    meta = json.loads((run_dir / "metadata.json").read_text())
    links = []
    for file in ["metadata.json","dataset_card.md","model_card.md","run_report.md",
                 "compliance_findings.json","compliance_summary.txt","model.joblib"]:
        if (run_dir / file).exists():
            links.append(f"<li><a href='/static/{run_id}/{file}'>{file}</a></li>")

    html = f"<h2>Run {run_id}</h2>"
    html += f"<p><b>Timestamp:</b> {meta['timestamp_utc']}</p>"
    html += f"<p><b>Compliance:</b> {meta.get('compliance',{}).get('status','-')}</p>"
    html += "<h3>Artefacts:</h3><ul>" + "".join(links) + "</ul>"
    return html

# Serve static run folders
app.mount("/static", StaticFiles(directory=str(base)), name="static")