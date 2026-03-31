import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "output" / "flarepocket.db"


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        source TEXT,
        slide_mode TEXT DEFAULT 'ignore',
        status TEXT DEFAULT 'pending',
        progress INTEGER DEFAULT 0,
        progress_text TEXT DEFAULT '待機中',
        created_at TEXT DEFAULT (datetime('now', 'localtime'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS issues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        timecode TEXT,
        rule TEXT,
        description TEXT,
        feedback TEXT DEFAULT NULL,
        FOREIGN KEY (job_id) REFERENCES jobs(id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS missed_issues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        timecode TEXT,
        rule TEXT,
        description TEXT,
        created_at TEXT DEFAULT (datetime('now', 'localtime')),
        FOREIGN KEY (job_id) REFERENCES jobs(id)
    )""")
    conn.commit()
    conn.close()


def create_job(job_id, source, slide_mode):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO jobs (id, source, slide_mode) VALUES (?, ?, ?)",
                 (job_id, source, slide_mode))
    conn.commit()
    conn.close()


def update_job(job_id, **kwargs):
    if not kwargs:
        return
    conn = sqlite3.connect(DB_PATH)
    fields = ", ".join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [job_id]
    conn.execute(f"UPDATE jobs SET {fields} WHERE id=?", values)
    conn.commit()
    conn.close()


def get_job(job_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def save_issues(job_id, issues):
    conn = sqlite3.connect(DB_PATH)
    for issue in issues:
        conn.execute(
            "INSERT INTO issues (job_id, timecode, rule, description) VALUES (?, ?, ?, ?)",
            (job_id, issue["timecode"], issue["rule"], issue["description"])
        )
    conn.commit()
    conn.close()


def get_report(job_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    issues = conn.execute(
        "SELECT * FROM issues WHERE job_id=? ORDER BY id", (job_id,)
    ).fetchall()
    missed = conn.execute(
        "SELECT * FROM missed_issues WHERE job_id=? ORDER BY created_at", (job_id,)
    ).fetchall()
    conn.close()
    return {"issues": [dict(r) for r in issues], "missed": [dict(r) for r in missed]}


def update_feedback(issue_id, feedback):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE issues SET feedback=? WHERE id=?", (feedback, issue_id))
    conn.commit()
    conn.close()


def add_missed_issue(job_id, timecode, rule, description):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO missed_issues (job_id, timecode, rule, description) VALUES (?, ?, ?, ?)",
        (job_id, timecode, rule, description)
    )
    conn.commit()
    conn.close()


def get_history(limit=30):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, source, status, progress_text, created_at FROM jobs ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
