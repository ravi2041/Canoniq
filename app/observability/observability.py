import os, json, time, uuid, logging, atexit, tempfile
from logging.handlers import RotatingFileHandler
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

# ---------- Runtime paths (safe) ----------
def _get_run_dir() -> str:
    """
    Pick a writable runtime directory.
    Priority:
      1) CANONIQ_RUN_DIR env var (lets you control location)
      2) project-root/.run (local dev)
      3) system temp (cloud-safe fallback)
    """
    env_dir = os.getenv("CANONIQ_RUN_DIR")
    if env_dir:
        return os.path.abspath(env_dir)

    # try project root: two levels up from this file if it's inside log_files/
    # adjust if you move this under app/observability/
    here = os.path.abspath(os.path.dirname(__file__))
    project_root = os.path.abspath(os.path.join(here, ".."))  # change to "..", ".." if needed
    run_dir = os.path.join(project_root, ".run")
    return run_dir

RUN_DIR = _get_run_dir()
LOG_DIR = os.path.join(RUN_DIR, "logs")
ARTIFACTS_DIR = os.path.join(RUN_DIR, "artifacts")

def _safe_makedirs(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception:
        return False

# Try to create our preferred dirs; if not possible, fall back to temp dir
if not (_safe_makedirs(LOG_DIR) and _safe_makedirs(ARTIFACTS_DIR)):
    RUN_DIR = os.path.join(tempfile.gettempdir(), "canoniq")
    LOG_DIR = os.path.join(RUN_DIR, "logs")
    ARTIFACTS_DIR = os.path.join(RUN_DIR, "artifacts")
    _safe_makedirs(LOG_DIR)
    _safe_makedirs(ARTIFACTS_DIR)

APP_LOG_PATH = os.path.join(LOG_DIR, "app.log")       # human-readable
PERF_LOG_PATH = os.path.join(LOG_DIR, "perf.jsonl")   # JSON lines timing

# ---------- App logger (human readable) ----------
_app_logger = logging.getLogger("canoniq.app")
_app_logger.setLevel(logging.INFO)

# ---------- Perf logger (JSON lines) ----------
_perf_logger = logging.getLogger("canoniq.perf")
_perf_logger.setLevel(logging.INFO)

def _add_console_handler(logger: logging.Logger) -> None:
    if any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        return
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(sh)

def _add_file_handler(logger: logging.Logger, path: str, json_raw: bool = False) -> None:
    # Avoid duplicate file handlers
    for h in logger.handlers:
        if isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == path:
            return

    try:
        fh = RotatingFileHandler(path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
        if not json_raw:
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(fh)
    except Exception:
        # If file logging fails (permissions, read-only FS), fall back to console
        _add_console_handler(logger)

# Prefer file logs locally, but always ensure something logs (console fallback)
if not _app_logger.handlers:
    _add_file_handler(_app_logger, APP_LOG_PATH, json_raw=False)
    _add_console_handler(_app_logger)  # keep console too (helps Streamlit logs)

if not _perf_logger.handlers:
    _add_file_handler(_perf_logger, PERF_LOG_PATH, json_raw=True)
    _add_console_handler(_perf_logger)

def app_log(msg: str, **kv):
    if kv:
        msg = f"{msg} | {json.dumps(kv, ensure_ascii=False)}"
    _app_logger.info(msg)

def perf_log(record: Dict[str, Any]):
    _perf_logger.info(json.dumps(record, ensure_ascii=False))

# ---------- Run/session ids ----------
def new_run_id(prefix: str = "run") -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{prefix}-{ts}-{short}"

# ---------- Artifacts ----------
def run_artifacts_dir(run_id: str) -> str:
    d = os.path.join(ARTIFACTS_DIR, run_id)
    os.makedirs(d, exist_ok=True)
    return d

def save_text_artifact(run_id: str, name: str, content: str, suffix: str = ".txt") -> str:
    path = os.path.join(run_artifacts_dir(run_id), f"{name}{suffix}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content if content is not None else "")
    app_log("artifact_saved", run_id=run_id, name=name, path=path)
    return path

def save_json_artifact(run_id: str, name: str, obj: Any) -> str:
    path = os.path.join(run_artifacts_dir(run_id), f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    app_log("artifact_saved", run_id=run_id, name=name, path=path)
    return path

# ---------- Timing ----------
@contextmanager
def time_block(step: str, run_id: Optional[str] = None, extra: Optional[Dict[str, Any]] = None):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        record = {"ts": datetime.utcnow().isoformat(), "run_id": run_id, "step": step, "seconds": round(dt, 6)}
        if extra:
            record.update(extra)
        perf_log(record)

# Ensure logs are flushed on exit
@atexit.register
def _shutdown_logs():
    for lg in (_app_logger, _perf_logger):
        for h in list(lg.handlers):
            try:
                h.flush()
                h.close()
            except Exception:
                pass
