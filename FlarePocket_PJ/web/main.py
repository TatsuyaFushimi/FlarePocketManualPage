import os
import shutil
import uuid
from pathlib import Path

# .env から環境変数を読み込む
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        if "=" in _line and not _line.startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

from fastapi import BackgroundTasks, FastAPI, File, Form, UploadFile
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from .database import (
    add_missed_issue,
    create_job,
    get_history,
    get_job,
    get_report,
    init_db,
    update_feedback,
)
from .processor import get_progress, process_job

BASE_DIR = Path(__file__).parent.parent
UPLOAD_DIR = BASE_DIR / "tmp" / "uploads"
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

app = FastAPI(title="FlarePocket")


@app.on_event("startup")
async def startup():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    init_db()


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.post("/submit")
async def submit(
    background_tasks: BackgroundTasks,
    url: str = Form(None),
    file: UploadFile = File(None),
    slide_mode: str = Form("ignore"),
    engine: str = Form("claude"),
):
    if not url and not file:
        return JSONResponse({"error": "URLまたはファイルが必要です"}, status_code=400)
    if engine not in ("claude", "gemini"):
        engine = "claude"

    job_id = uuid.uuid4().hex[:8]

    if url:
        source = ("url", url.strip())
    else:
        dest = UPLOAD_DIR / f"{job_id}_{file.filename}"
        with open(dest, "wb") as f:
            shutil.copyfileobj(file.file, f)
        source = ("file", str(dest))

    create_job(job_id, source[1], slide_mode)
    background_tasks.add_task(process_job, job_id, source, slide_mode, engine)
    return {"job_id": job_id}


@app.get("/jobs/{job_id}/status")
async def job_status(job_id: str):
    # Prefer in-memory progress (more real-time), fall back to DB
    mem = get_progress(job_id)
    if mem:
        return mem
    job = get_job(job_id)
    if not job:
        return JSONResponse({"error": "not found"}, status_code=404)
    return {"status": job["status"], "progress": job["progress"], "text": job["progress_text"]}


@app.get("/jobs/{job_id}/report")
async def job_report(job_id: str):
    report = get_report(job_id)
    if report is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return report


@app.post("/issues/{issue_id}/feedback")
async def issue_feedback(issue_id: int, body: dict):
    fb = body.get("feedback")  # "correct" | "false_positive"
    if fb not in ("correct", "false_positive", None):
        return JSONResponse({"error": "invalid feedback"}, status_code=400)
    update_feedback(issue_id, fb)
    return {"ok": True}


@app.post("/jobs/{job_id}/missed")
async def add_missed(job_id: str, body: dict):
    add_missed_issue(
        job_id,
        body.get("timecode", ""),
        body.get("rule", ""),
        body.get("description", ""),
    )
    return {"ok": True}


@app.get("/history")
async def history():
    return get_history()
