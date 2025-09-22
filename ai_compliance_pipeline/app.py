from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import os, json
from azure.storage.blob import BlobServiceClient

app = FastAPI(title="AI Compliance Pipeline Viewer")


base = Path(__file__).parent / "artifacts" / "runs"


AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = "artifacts"
blob_service = None
if AZURE_CONN_STR:
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)


@app.get("/", response_class=HTMLResponse)
async def home():
    return '<h2>Go to <a href="/runs">/runs</a> to view pipeline runs</h2>'


@app.get("/runs", response_class=HTMLResponse)
async def list_runs():
    """List all runs either from Blob Storage or local artifacts."""
    rows = []
    if blob_service:
        container = blob_service.get_container_client(AZURE_CONTAINER)
        blobs = container.list_blobs()
        seen = set()
        for b in blobs:
            parts = b.name.split("/")
            if len(parts) >= 2 and parts[0] == "runs":
                run_id = parts[1]
                if run_id not in seen:
                    seen.add(run_id)
                    meta_blob = container.get_blob_client(f"runs/{run_id}/metadata.json")
                    try:
                        meta = json.loads(meta_blob.download_blob().readall())
                        rows.append(
                            f"<tr><td>{meta['run_id']}</td>"
                            f"<td>{meta['timestamp_utc']}</td>"
                            f"<td>{meta.get('model',{}).get('metrics',{}).get('value','-')}</td>"
                            f"<td>{meta.get('compliance',{}).get('status','-')}</td>"
                            f"<td><a href='/runs/{meta['run_id']}'>View</a></td></tr>"
                        )
                    except Exception:
                        pass
    else:
        for run_dir in sorted(base.iterdir(), reverse=True):
            meta_path = run_dir / "metadata.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text())
                rows.append(
                    f"<tr><td>{meta['run_id']}</td>"
                    f"<td>{meta['timestamp_utc']}</td>"
                    f"<td>{meta.get('model',{}).get('metrics',{}).get('value','-')}</td>"
                    f"<td>{meta.get('compliance',{}).get('status','-')}</td>"
                    f"<td><a href='/runs/{meta['run_id']}'>View</a></td></tr>"
                )

    table = (
        "<table border=1><tr><th>Run ID</th><th>Time</th><th>Accuracy</th><th>Compliance</th><th>Link</th></tr>"
    )
    table += "".join(rows) + "</table>"
    return table


@app.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_detail(run_id: str):
    """Show details + artefact links for a specific run."""
    if blob_service:
        container = blob_service.get_container_client(AZURE_CONTAINER)
        meta_blob = container.get_blob_client(f"runs/{run_id}/metadata.json")
        if not meta_blob.exists():
            return f"<p>No run {run_id} found in Blob.</p>"
        meta = json.loads(meta_blob.download_blob().readall())
        links = []
        for fname in [
            "metadata.json",
            "dataset_card.md",
            "model_card.md",
            "run_report.md",
            "compliance_findings.json",
            "compliance_summary.txt",
            "model.joblib",
        ]:
            blob = container.get_blob_client(f"runs/{run_id}/{fname}")
            if blob.exists():
                url = blob.url
                links.append(f"<li><a href='{url}'>{fname}</a></li>")
        html = f"<h2>Run {run_id}</h2>"
        html += f"<p><b>Timestamp:</b> {meta['timestamp_utc']}</p>"
        html += f"<p><b>Compliance:</b> {meta.get('compliance',{}).get('status','-')}</p>"
        html += "<h3>Artefacts:</h3><ul>" + "".join(links) + "</ul>"
        return html
    else:
        run_dir = base / run_id
        if not run_dir.exists():
            return f"<p>No run {run_id} found locally.</p>"
        meta = json.loads((run_dir / "metadata.json").read_text())
        links = []
        for file in [
            "metadata.json",
            "dataset_card.md",
            "model_card.md",
            "run_report.md",
            "compliance_findings.json",
            "compliance_summary.txt",
            "model.joblib",
        ]:
            if (run_dir / file).exists():
                links.append(f"<li><a href='/static/{run_id}/{file}'>{file}</a></li>")
        html = f"<h2>Run {run_id}</h2>"
        html += f"<p><b>Timestamp:</b> {meta['timestamp_utc']}</p>"
        html += f"<p><b>Compliance:</b> {meta.get('compliance',{}).get('status','-')}</p>"
        html += "<h3>Artefacts:</h3><ul>" + "".join(links) + "</ul>"
        return html



if not blob_service:
    app.mount("/static", StaticFiles(directory=str(base)), name="static")
