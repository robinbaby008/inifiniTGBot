#!/usr/bin/env python3
"""Telegram bot: send a download link -> streams directly to InfiniCloud WebDAV.

No local disk usage. Download is piped chunk-by-chunk into a WebDAV PUT.
Works on PC / VPS / Termux (Android).

Setup:
  1. cp .env.example .env  (then fill in values)
  2. pip install -r requirements.txt   (Termux: pkg install python && pip install -r requirements.txt)
  3. python bot.py

Env vars (in .env):
  TELEGRAM_BOT_TOKEN   from @BotFather
  WEBDAV_URL           e.g. https://XXXX.infini-cloud.net/dav/  (must end with /)
  WEBDAV_USERNAME      InfiniCloud user ID
  WEBDAV_PASSWORD      InfiniCloud password (app password if you use one)
  WEBDAV_DIR           optional subfolder, e.g. Telegram  (empty = root)
  ALLOWED_USER_IDS     optional comma-separated Telegram IDs, empty = allow all
"""

import argparse
import asyncio
import itertools
import json
import logging
import logging.handlers
import os
import re
import threading
import time
import urllib.parse
from email.utils import parsedate_to_datetime
from pathlib import PurePosixPath

import requests
from dotenv import load_dotenv
from telegram import BotCommand, Update
from telegram.error import Conflict
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from telegram.request import HTTPXRequest

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBDAV_URL = os.getenv("WEBDAV_URL", "").strip()
WEBDAV_USERNAME = os.getenv("WEBDAV_USERNAME", "").strip()
WEBDAV_PASSWORD = os.getenv("WEBDAV_PASSWORD", "").strip()
WEBDAV_DIR = os.getenv("WEBDAV_DIR", "").strip().strip("/")
ALLOWED_IDS_RAW = os.getenv("ALLOWED_USER_IDS", "").strip()

if not WEBDAV_URL.endswith("/"):
    WEBDAV_URL += "/"

ALLOWED_IDS = set()
if ALLOWED_IDS_RAW:
    for part in ALLOWED_IDS_RAW.split(","):
        part = part.strip()
        if part.isdigit():
            ALLOWED_IDS.add(int(part))

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
log = logging.getLogger("infi-tg")

URL_RE = re.compile(r"https?://[^\s<>\"]+", re.IGNORECASE)
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
CHUNK = 256 * 1024  # 256 KB per chunk
MAX_CONCURRENT = max(1, int(os.getenv("MAX_CONCURRENT_UPLOADS", "3")))
MAX_LINKS_PER_MSG = max(1, int(os.getenv("MAX_LINKS_PER_MESSAGE", "10")))

# Telegram's Bot API will not hand out a download URL for anything bigger than
# this: getFile simply answers "File is too big". It is a platform limit, not a
# bot setting -- a forwarded file over this size cannot be fetched by any bot,
# however it is forwarded. Only a direct link gets around it, and those have no
# size limit. Documented at core.telegram.org/bots/api#getfile
TG_FILE_MAX_BYTES = 20 * 1000 * 1000

# --- Telegram API timeouts ---
# python-telegram-bot defaults all of these to 5s (1s for the pool wait), which
# a VPN, a mobile link or a stuttering tunnel blows through -- and a single blown
# timeout during startup used to crash the whole bot. Generous on purpose: a
# Telegram API call taking 30s is slow, not broken. read_timeout must comfortably
# exceed the long-poll window or every getUpdates would time out.
TG_CONNECT_TIMEOUT = float(os.getenv("TG_CONNECT_TIMEOUT", "30"))
TG_READ_TIMEOUT = float(os.getenv("TG_READ_TIMEOUT", "60"))
TG_POOL_TIMEOUT = float(os.getenv("TG_POOL_TIMEOUT", "30"))

# IMPORTANT requests gotcha: a (connect, read) tuple does NOT set a separate
# write timeout. urllib3 keeps the socket at the *connect* value while it
# streams the request body (only after the body is sent does it switch the
# socket to the read value). So a small connect timeout = a small write timeout:
# if teracloud's receive window fills up and TCP back-pressures us, sendall()
# blocks and the upload dies with "The write operation timed out".
# That is exactly how 2 parallel 5 GB uploads both died ~13 s apart.
# So the first element must be a stall tolerance, not a handshake timeout.
STALL_TIMEOUT = max(30, int(os.getenv("STALL_TIMEOUT", "600")))  # 10 min
PUT_TIMEOUT = (STALL_TIMEOUT, 60 * 60 * 6)   # (stall/write, read)
GET_TIMEOUT = (15, int(os.getenv("SOURCE_READ_TIMEOUT", "60")))

# Optional hard bandwidth cap for uploads, in MB/s. 0 / unset = unlimited.
# Set it BELOW your real uplink speed: then teracloud's receive window never
# fills, and the back-pressure failure described above cannot happen at all.
# Shared by every concurrent job, so N jobs at a cap of M still total M.
MAX_UPLOAD_SPEED_MBS = max(0.0, float(os.getenv("MAX_UPLOAD_SPEED_MBS", "0")))

# --- cleanup / retention ---
JOB_LOG_MAX_BYTES = max(1, int(os.getenv("JOB_LOG_MAX_BYTES", str(20 * 1024 * 1024))))
JOB_LOG_KEEP = max(100, int(os.getenv("JOB_LOG_KEEP", "5000")))
LOG_MAX_BYTES = max(1, int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024))))
LOG_BACKUPS = max(0, int(os.getenv("LOG_BACKUPS", "3")))
CLEAN_MAX_AGE_HOURS = float(os.getenv("CLEAN_MAX_AGE_HOURS", "24"))
CLEAN_MAX_DELETES = max(1, int(os.getenv("CLEAN_MAX_DELETES", "200")))
CLEAN_WEAK = os.getenv("CLEAN_WEAK", "1").strip().lower() not in ("0", "false", "no")
# Anything younger than this may still be an in-flight upload; never touch it.
CLEAN_MIN_FREE_SECS = 60
ORPHAN_TINY_BYTES = 1024 * 1024  # weak signal ceiling: 1 MB
LOG_PATH = os.getenv("BOT_LOG_PATH", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "bot.log"))


def setup_file_log(path: str) -> None:
    """Log to a size-rotated file instead of stdout.

    Without this the bot only grows when the shell redirect is set up by hand,
    and nothing ever trims it.
    """
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.handlers.RotatingFileHandler(
        path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    root.addHandler(handler)
    # PTB logs full request URLs at INFO, which embeds the bot token in the path.
    logging.getLogger("httpx").setLevel(logging.WARNING)


# ---------- job registry (multi-upload / progress / cancel) ----------

class UploadCancelled(Exception):
    """Raised inside the worker thread when a job is cancelled."""


JOBS: dict[int, dict] = {}
HISTORY: dict[int, dict] = {}  # finished jobs, so /jobs can still report them
HISTORY_MAX = 20
_JOB_IDS = itertools.count(1)
_JOB_LOCK = threading.Lock()
JOB_SEM: asyncio.Semaphore | None = None  # bound to the event loop in main()
BACKGROUND: set[asyncio.Task] = set()  # strong refs, so uploads aren't GC'd


def spawn(coro) -> asyncio.Task:
    """Run a handler's work detached so it never blocks the update loop.

    Required for /jobs and /cancel to stay responsive during a long upload:
    the bot processes updates sequentially, so a handler that awaits the whole
    upload would freeze every other command until it finished.
    """
    task = asyncio.create_task(coro)
    BACKGROUND.add(task)
    task.add_done_callback(BACKGROUND.discard)
    return task


def new_job(user_id: int, url: str, filename: str) -> dict:
    job = {
        "id": 0,
        "user_id": user_id,
        "url": url,
        "progress": {"done": 0, "total": 0, "filename": filename, "stage": "queued"},
        "cancel": threading.Event(),
        "started": time.time(),
        "finished": False,
        "result": None,
        "error": None,
    }
    with _JOB_LOCK:
        job["id"] = next(_JOB_IDS)
        JOBS[job["id"]] = job
    return job


def get_job(job_id: int) -> dict | None:
    with _JOB_LOCK:
        return JOBS.get(job_id)


def finish_job(job: dict, result: dict | None = None, error: str | None = None):
    with _JOB_LOCK:
        job["finished"] = True
        job["result"] = result
        job["error"] = error
        JOBS.pop(job["id"], None)
        # Keep a short tail so /jobs can still answer "what happened to the
        # other job?" instead of the entry silently disappearing.
        HISTORY[job["id"]] = job
        while len(HISTORY) > HISTORY_MAX:
            HISTORY.pop(min(HISTORY))


def request_cancel(job: dict) -> None:
    """Ask the worker thread to stop. Takes effect at the next chunk boundary."""
    job["cancel"].set()


# ---------- dest-name reservation (same-name race) ----------

_NAME_LOCK = threading.Lock()
_RESERVED_NAMES: set[str] = set()


def reserve_dest_name(dav: requests.Session, fname: str) -> str:
    """Pick and reserve a free dest name, atomically against other jobs.

    The lock spans the whole HEAD probe, which is what makes this safe. Without
    it, two concurrent jobs uploading same-named files can both observe 404 for
    the same name and then PUT into the same path, interleaving into one
    corrupt file. Reservation is released when the job ends.
    """
    stem, dot, suffix = fname.rpartition(".")
    with _NAME_LOCK:
        counter = 0
        probe = fname
        while True:
            if probe not in _RESERVED_NAMES:
                try:
                    h = dav.head(webdav_join(WEBDAV_DIR, probe), timeout=15)
                    if h.status_code == 404:
                        break
                    if h.status_code not in (200, 301, 302):
                        break  # auth error etc: don't spin
                except Exception:
                    break
            counter += 1
            if counter > 100:
                # Old code broke out here with dest_url still pointing at the
                # *original* name, which silently overwrote an existing file.
                # Commit to the probed name instead.
                break
            probe = f"{stem}_{counter}.{suffix}" if dot else f"{fname}_{counter}"
        _RESERVED_NAMES.add(probe)
    return probe


def release_dest_name(name: str) -> None:
    with _NAME_LOCK:
        _RESERVED_NAMES.discard(name)


# ---------- durable job audit log ----------

JOB_LOG_PATH = os.getenv("JOB_LOG_PATH", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "jobs.jsonl"))
_JOB_LOG_LOCK = threading.Lock()


def compact_job_log(keep: int | None = None,
                    max_bytes: int | None = None) -> tuple[int, int]:
    """Rewrite jobs.jsonl keeping the newest `keep` records.

    `max_bytes` tightens that further. A record-count limit alone cannot honour
    a byte budget when the surviving records are themselves large, so the tail
    is trimmed until it fits. Returns (lines_before, lines_after).

    Uses a temp file + os.replace so a crash mid-compaction cannot truncate the
    audit trail.
    """
    keep = JOB_LOG_KEEP if keep is None else keep
    try:
        with open(JOB_LOG_PATH, "r", encoding="utf-8") as fh:
            lines = [ln for ln in fh.read().splitlines() if ln.strip()]
    except FileNotFoundError:
        return 0, 0
    before = len(lines)
    tail = lines[-keep:]
    if max_bytes:
        size = sum(len(ln) + 1 for ln in tail)
        while len(tail) > 1 and size > max_bytes:
            size -= len(tail[0]) + 1
            tail = tail[1:]
    if len(tail) == before:
        return before, before
    tmp = f"{JOB_LOG_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(tail) + "\n")
    os.replace(tmp, JOB_LOG_PATH)
    return before, len(tail)


def record_event(job: dict, event: str, **extra):
    """Append one event per job transition to a JSONL file.

    Without this, successful uploads left no on-disk trace at all, and the
    in-memory HISTORY is wiped on every restart.
    """
    p = job["progress"]
    rec = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "job": job["id"],
        "user": job["user_id"],
        "event": event,
        "url": job["url"],
        "filename": extra.get("filename") or p.get("filename", ""),
        "dest_url": extra.get("dest_url") or p.get("dest_url"),
        "done": p.get("done", 0),
        "total": p.get("total", 0),
        "seconds": round(extra.get("seconds", 0), 1),
        "read_s": extra.get("read_s"),
        "write_s": extra.get("write_s"),
        "error": extra.get("error"),
    }
    line = json.dumps(rec, ensure_ascii=False)
    with _JOB_LOG_LOCK:
        try:
            with open(JOB_LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            # Auto-compact so a long-running bot can't grow the file forever.
            try:
                if os.path.getsize(JOB_LOG_PATH) > JOB_LOG_MAX_BYTES:
                    before, after = compact_job_log(max_bytes=JOB_LOG_MAX_BYTES)
                    log.info("compacted %s: %d -> %d records", JOB_LOG_PATH, before, after)
            except OSError:
                pass
        except Exception as e:
            log.warning("could not append to %s: %s", JOB_LOG_PATH, e)
    return rec


def emit_started(progress: dict, fname: str, dest_url: str):
    """Log the 'started' transition, once the final dest name is known.

    A started record with no terminal record afterwards means the process died
    mid-transfer, which is exactly the case /clean needs to identify.
    """
    job = progress.get("_job")
    if job is None:
        return
    record_event(job, "started", filename=fname, dest_url=dest_url,
                 seconds=round(time.time() - job["started"], 1))


def read_history(user_id: int | None, limit: int = 20) -> list[dict]:
    """The last `limit` matching records, in log order (oldest first).

    Walks the file BACKWARDS in blocks and stops as soon as it has enough,
    then flips back to log order for the caller. This is called from async
    command handlers, and jobs.jsonl is allowed to grow to JOB_LOG_MAX_BYTES
    (20 MB ~ 85k records), where parsing it front to back costs ~450 ms -- all
    of it on the event loop, stalling progress updates and /cancel for a
    running upload just to answer /history.

    Log order (not newest first) is what callers expect: cmd_history reverses
    it for display, and find_orphans builds a filename index either way.
    """
    if limit <= 0:
        return []
    out: list[dict] = []
    try:
        with open(JOB_LOG_PATH, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            pos = fh.tell()
            buf = b""
            block = 64 * 1024
            while pos > 0 and len(out) < limit:
                step = min(block, pos)
                pos -= step
                fh.seek(pos)
                buf = fh.read(step) + buf
                lines = buf.split(b"\n")
                # lines[0] is the oldest piece and may be a partial line whose
                # start is in the previous block, so carry it forward.
                buf = lines.pop(0)
                for raw in reversed(lines):
                    if not raw.strip():
                        continue
                    try:
                        rec = json.loads(raw.decode("utf-8"))
                    except Exception:
                        continue
                    # A log line is not guaranteed to be an object: a stray "5"
                    # parses fine and would blow up .get() below.
                    if not isinstance(rec, dict):
                        continue
                    if user_id is None or rec.get("user") == user_id:
                        out.append(rec)
                        if len(out) >= limit:
                            break
            # Reached the start of the file: whatever is left in buf is its
            # first line, which never got a trailing newline to split on.
            if len(out) < limit and buf.strip():
                try:
                    rec = json.loads(buf.decode("utf-8"))
                except Exception:
                    rec = None
                if isinstance(rec, dict) and (user_id is None or rec.get("user") == user_id):
                    out.append(rec)
    except FileNotFoundError:
        return []
    out.reverse()  # collected newest -> oldest; hand back log order
    return out


def jobs_for_user(user_id: int) -> list[dict]:
    with _JOB_LOCK:
        return sorted(
            (j for j in JOBS.values() if j["user_id"] == user_id),
            key=lambda j: j["id"],
        )


def recent_for_user(user_id: int, limit: int = 5) -> list[dict]:
    with _JOB_LOCK:
        done = sorted(
            (j for j in HISTORY.values() if j["user_id"] == user_id),
            key=lambda j: j["id"],
        )
    return done[-limit:]


def job_outcome(job: dict) -> str:
    """One-line verdict for a finished job."""
    mins = (time.time() - job["started"]) / 60
    name = (job.get("result") or {}).get("filename") or job["progress"].get("filename", "?")
    name = str(name)[:40]
    if job.get("error") == "cancelled":
        return f"  #{job['id']} 🚫 cancelled — {name} ({fmt_size(job['progress'].get('done', 0))} discarded)"
    if job.get("error"):
        return f"  #{job['id']} ❌ failed after {mins:.0f}m — {name}\n      {job['error'][:120]}"
    return f"  #{job['id']} ✅ done — {name} ({fmt_size((job.get('result') or {}).get('size', 0))})"


def browser_headers(url: str) -> dict:
    parsed = urllib.parse.urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"
    return {
        "User-Agent": BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Upgrade-Insecure-Requests": "1",
        # requests otherwise advertises "gzip, deflate, zstd". Almost everything
        # this bot moves is already-compressed media (mkv/mp4/zip), so the source
        # either serves it as-is or burns CPU re-gzipping megabytes we will only
        # push straight back out again. Identity avoids both.
        "Accept-Encoding": "identity",
    }


# ---------- helpers ----------

def is_allowed(update: Update) -> bool:
    if not ALLOWED_IDS:
        return True
    uid = update.effective_user.id if update.effective_user else None
    return uid in ALLOWED_IDS


def safe_filename(name: str, fallback: str = "file") -> str:
    """Strip path tricks, query strings, illegal chars."""
    name = urllib.parse.unquote(name or "").strip()
    name = name.split("?")[0].split("#")[0]
    name = PurePosixPath(name).name  # drop directories
    name = re.sub(r'[\\/:*?"<>|]+', "_", name).strip(" .")
    if not name or name in (".", ".."):
        name = fallback
    return name[:200]


def filename_from_url(url: str) -> str:
    path = urllib.parse.urlparse(url).path
    base = PurePosixPath(path).name
    name = safe_filename(base, "")
    if not name:
        # no usable name in URL, e.g. https://example.com/download?id=123
        ts = time.strftime("%Y%m%d-%H%M%S")
        name = f"download-{ts}"
    return name


def filename_from_cd(content_disposition: str | None, default: str) -> str:
    """Parse Content-Disposition: attachment; filename*=UTF-8''... or filename=\"...\"."""
    if not content_disposition:
        return default
    # RFC 5987 filename*
    m = re.search(r"filename\*\s*=\s*([^;]+)", content_disposition, re.IGNORECASE)
    if m:
        val = m.group(1).strip().strip("'\"")
        # format: UTF-8''encoded
        if "''" in val:
            _, _, encoded = val.partition("''")
            try:
                return safe_filename(urllib.parse.unquote(encoded), default)
            except Exception:
                pass
        else:
            return safe_filename(urllib.parse.unquote(val), default)
    m = re.search(r'filename\s*=\s*"([^"]+)"', content_disposition, re.IGNORECASE)
    if m:
        return safe_filename(m.group(1), default)
    m = re.search(r"filename\s*=\s*([^;\s]+)", content_disposition, re.IGNORECASE)
    if m:
        return safe_filename(m.group(1).strip().strip("'\""), default)
    return default


def webdav_join(*parts: str) -> str:
    base = WEBDAV_URL
    rel = "/".join(p.strip("/") for p in parts if p and p.strip("/"))
    if rel:
        return base + "/".join(urllib.parse.quote(p, safe="") for p in rel.split("/"))
    return base


_WEBDAV_DIRS_READY: set[str] = set()


def ensure_webdav_dir(session: requests.Session, dest_dir_url: str):
    """MKCOL each level of WEBDAV_DIR. Ignore 405/201/200 (already exists).

    Successful collections are remembered: a MKCOL is one round trip per level
    per job, which is pure overhead once the directory exists (and the bot can
    run for days). Only a clear "this does not exist" answer is cached, so a
    collection removed underneath us is still recreated.
    """
    if not WEBDAV_DIR:
        return
    accum = WEBDAV_URL
    for part in WEBDAV_DIR.split("/"):
        part = part.strip()
        if not part:
            continue
        accum = accum + urllib.parse.quote(part, safe="") + "/"
        if accum in _WEBDAV_DIRS_READY:
            continue
        try:
            r = session.request("MKCOL", accum, timeout=20)
            if r.status_code in (201, 200, 405, 409):
                _WEBDAV_DIRS_READY.add(accum)
                continue
            log.warning("MKCOL %s -> %s", accum, r.status_code)
        except Exception as e:
            log.warning("MKCOL %s failed: %s", accum, e)


def resolve_with_ytdlp(source_url: str) -> tuple[str, str]:
    """Try yt-dlp to resolve a page/downloader link to a direct media URL.

    Returns (direct_url, title). Raises RuntimeError if not resolvable.
    """
    import shutil
    import subprocess

    if not shutil.which("yt-dlp"):
        raise RuntimeError(
            "This link needs yt-dlp to resolve (it's a web page, not a direct file). "
            "Install it: pip install yt-dlp  and try again."
        )
    try:
        out = subprocess.run(
            ["yt-dlp", "-g", "--no-playlist", "--no-warnings", source_url],
            capture_output=True, text=True, timeout=60,
        )
    except FileNotFoundError:
        raise RuntimeError("yt-dlp not found. Install it: pip install yt-dlp")
    if out.returncode != 0 or not out.stdout.strip():
        err = (out.stderr or "")[-500:]
        raise RuntimeError(f"yt-dlp could not resolve this link. {err}")
    direct = out.stdout.strip().splitlines()[0].strip()
    title = ""
    try:
        t = subprocess.run(
            ["yt-dlp", "--get-title", "--no-playlist", "--no-warnings", source_url],
            capture_output=True, text=True, timeout=30,
        )
        if t.returncode == 0 and t.stdout.strip():
            title = safe_filename(t.stdout.strip().splitlines()[0], "")
    except Exception:
        pass
    return direct, title


def do_stream_upload(source_url: str, progress: dict, cancel: threading.Event | None = None) -> dict:
    """Blocking: GET source_url (stream) -> PUT to WebDAV (stream). No disk I/O.

    progress dict is mutated: {done, total, filename, stage, dest_url, dest_name}
    cancel is checked every chunk; raises UploadCancelled and removes the partial.
    Returns {filename, size, webdav_url, webdav_path}
    Raises on HTTP error.
    """
    dl = requests.Session()  # no auth! never send WebDAV creds to 3rd-party hosts
    dav = requests.Session()
    dav.auth = (WEBDAV_USERNAME, WEBDAV_PASSWORD)
    try:
        return _do_stream_upload(dl, dav, source_url, progress, cancel)
    finally:
        # Sessions are not reachable from the error paths, so without this every
        # job leaks a urllib3 connection pool and its keep-alive sockets until
        # the GC gets round to it.
        dl.close()
        dav.close()
        # Released here rather than by the caller, so the name is always freed
        # even if the caller returns early. Idempotent, so double-release is safe.
        release_dest_name(progress.get("dest_name", ""))


def _do_stream_upload(dl: requests.Session, dav: requests.Session, source_url: str,
                      progress: dict, cancel: threading.Event | None) -> dict:
    """Worker behind do_stream_upload. Sessions are owned by the caller."""

    def check_cancel():
        if cancel is not None and cancel.is_set():
            raise UploadCancelled()

    ensure_webdav_dir(dav, WEBDAV_URL)

    progress["stage"] = "connecting"
    headers = browser_headers(source_url)
    check_cancel()
    try:
        src_resp = dl.get(source_url, stream=True, timeout=GET_TIMEOUT,
                          allow_redirects=True, headers=headers)
    except requests.RequestException as e:
        raise RuntimeError(f"Could not connect to link: {e}") from e

    # 403 / Cloudflare challenge -> try yt-dlp fallback before giving up
    if src_resp.status_code == 403 or "cf-mitigated" in (src_resp.headers.get("CF-Mitigated", "").lower()):
        body_start = ""
        try:
            body_start = src_resp.text[:2000]
        except Exception:
            pass
        src_resp.close()
        lowered = body_start.lower()
        is_cf = ("just a moment" in lowered or "cloudflare" in lowered
                 or "cf-chl" in lowered or "turnstile" in lowered)
        if is_cf:
            progress["stage"] = "resolving via yt-dlp"
            log.info("Cloudflare challenge detected, trying yt-dlp for %s", source_url)
            try:
                direct, title = resolve_with_ytdlp(source_url)
                if title:
                    progress["filename"] = title
                # retry with resolved direct URL (recursion, one level)
                return do_stream_upload(direct, progress, cancel)
            except RuntimeError as ye:
                raise RuntimeError(
                    "❌ This site is protected by Cloudflare ('Just a moment...').\n"
                    "The bot can't open it directly.\n\n"
                    "Fixes:\n"
                    "1. Open the link in your browser, wait for download to start, "
                    "copy the *final* .mp3/.mp4 CDN URL and send that.\n"
                    "2. Or install yt-dlp and send a supported page URL.\n\n"
                    f"Detail: {ye}"
                ) from ye
        raise RuntimeError(
            f"Download failed (403 Forbidden). The host rejected a bot request. "
            f"Open it in a browser and send the final direct file URL. Hint: {body_start[:200]}"
        )

    try:
        src_resp.raise_for_status()
    except requests.HTTPError as e:
        hint = ""
        try:
            hint = src_resp.text[:300]
        except Exception:
            pass
        code = src_resp.status_code
        src_resp.close()
        # generic fallback: page links (youtube, etc.) may work via yt-dlp
        if code in (401, 403, 404):
            raise RuntimeError(f"Download failed ({code}): {hint[:200]}") from e
        raise RuntimeError(f"Download failed ({code}): {hint[:200]}") from e

    with src_resp as src:
        total = int(src.headers.get("Content-Length") or 0)
        ctype = src.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
        cd = src.headers.get("Content-Disposition")

        # Reject HTML pages early: this is a web page, not a file
        if "text/html" in ctype:
            try:
                snippet = src.text[:2000]
            except Exception:
                snippet = ""
            low = snippet.lower()
            if "just a moment" in low or "cloudflare" in low:
                raise RuntimeError(
                    "❌ This link returned a Cloudflare challenge page, not a file.\n"
                    "Open it in your browser and send the *final* direct download URL."
                )
            raise RuntimeError(
                "❌ This link is a web page, not a direct file.\n"
                "Open it in your browser, start the download, and send the final direct URL "
                "(or install yt-dlp: pip install yt-dlp — the bot will then auto-resolve it)."
            )

        base_name = filename_from_url(src.url)  # src.url = final URL after redirects
        fname = filename_from_cd(cd, base_name)
        # add extension from content-type if missing and type is obvious
        if "." not in fname:
            ext = {
                "video/mp4": ".mp4", "video/x-matroska": ".mkv",
                "video/webm": ".webm", "audio/mpeg": ".mp3",
                "application/zip": ".zip", "application/pdf": ".pdf",
                "image/jpeg": ".jpg", "image/png": ".png",
            }.get(ctype)
            if ext:
                fname += ext

        dest_url = webdav_join(WEBDAV_DIR, fname)
        # Avoid overwrite, and reserve the name so a concurrent job with the same
        # filename cannot pick it too (see reserve_dest_name).
        fname = reserve_dest_name(dav, fname)
        dest_url = webdav_join(WEBDAV_DIR, fname)

        progress.update(filename=fname, total=total, done=0, stage="uploading")
        # Recorded so a later /clean can prove a partial from the remote size.
        progress["dest_url"] = dest_url
        progress["dest_name"] = fname
        emit_started(progress, fname, dest_url)
        log.info("PUT %s | size=%s type=%s (src=%s)",
                 dest_url, fmt_size(total) if total else "unknown", ctype, src.url)

        put_headers = {"Content-Type": ctype or "application/octet-stream"}

        progress["stage"] = "uploading"
        check_cancel()
        put = None
        read_s = 0.0
        t_put = time.monotonic()
        try:
            if total > 0:
                # Sized stream -> requests sends Content-Length, no chunked encoding.
                # (Old code used a generator + manual Content-Length which sends
                #  BOTH Content-Length and Transfer-Encoding: chunked -> Apache 400.)
                stream = _UploadStream(src, progress, cancel)
                put = dav.put(dest_url, data=stream, headers=put_headers,
                              timeout=PUT_TIMEOUT)
                downloaded = stream.done
                read_s = stream.timing["read_s"]
            else:
                # Unknown size -> true chunked upload
                downloaded = 0

                def gen():
                    nonlocal downloaded, read_s
                    it = src.iter_content(chunk_size=CHUNK)
                    while True:
                        t0 = time.monotonic()
                        try:
                            chunk = next(it)
                        except StopIteration:
                            read_s += time.monotonic() - t0
                            return
                        read_s += time.monotonic() - t0
                        if not chunk:
                            continue
                        check_cancel()
                        downloaded += len(chunk)
                        progress["done"] = downloaded
                        _BANDWIDTH.consume(len(chunk))
                        yield chunk

                put = dav.put(dest_url, data=gen(), headers=put_headers,
                              timeout=PUT_TIMEOUT)
            put.raise_for_status()
        except Exception as e:
            # requests may surface the body-read failure wrapped; the cancel flag
            # is the source of truth, so check it before reporting a real error.
            if cancel is not None and cancel.is_set():
                verdict = delete_partial_if_incomplete(dav, dest_url, total)
                log.info("cancelled %s (%s) -> %s", dest_url, fname, verdict)
                raise UploadCancelled() from e
            if isinstance(e, requests.HTTPError):
                log.error("PUT failed %s dest=%s req-headers=%s resp-headers=%s body=%s",
                          put.status_code, dest_url,
                          dict(put.request.headers), dict(put.headers), put.text[:500])
                delete_partial_if_incomplete(dav, dest_url, total)
                raise RuntimeError(f"WebDAV PUT failed ({put.status_code}): {put.text[:300]}") from e
            # Timeout / connection reset / protocol error: usually leaves a
            # truncated object behind, so sweep it before re-raising.
            delete_partial_if_incomplete(dav, dest_url, total)
            raise

        # http.client.send() only ever does read() then sendall(), so whatever
        # the PUT took that was NOT spent blocked on the source socket was spent
        # writing to teracloud. That split is the whole diagnostic.
        put_s = time.monotonic() - t_put
        timing = {"read_s": round(read_s, 2),
                  "write_s": round(max(0.0, put_s - read_s), 2)}

        webdav_path = f"{WEBDAV_DIR}/{fname}" if WEBDAV_DIR else fname
        return {"filename": fname, "size": downloaded, "total": total,
                "webdav_url": dest_url, "webdav_path": webdav_path,
                "timing": timing}


def delete_quietly(session: requests.Session, url: str):
    """Remove a partial/failed upload. Never raises."""
    try:
        session.delete(url, timeout=20)
    except Exception as e:
        log.warning("DELETE %s failed: %s", url, e)


def delete_partial_if_incomplete(session: requests.Session, url: str,
                                 expected_total: int) -> str:
    """Remove a partial upload, but never destroy a file the server committed.

    A failed PUT is ambiguous: the server may have discarded the truncated body,
    stored it, or - if only our *response* was lost - stored the whole thing.
    Deleting blindly would throw away a perfectly good multi-GB file, so we HEAD
    first and compare against what the job was supposed to send.

    Returns "absent" | "kept-committed" | "deleted" | "unknown".
    """
    try:
        h = session.head(url, timeout=20)
    except Exception as e:
        log.warning("HEAD %s before cleanup failed: %s", url, e)
        return "unknown"
    if h.status_code == 404:
        return "absent"
    if h.status_code not in (200, 301, 302):
        return "unknown"
    raw = h.headers.get("Content-Length") or "0"
    remote = int(raw) if raw.strip().isdigit() else 0
    if expected_total > 0 and remote == expected_total:
        log.info("keeping %s: server committed all %d bytes", url, remote)
        return "kept-committed"
    log.info("removing partial %s: %d of %s bytes present",
             url, remote, expected_total or "?")
    delete_quietly(session, url)
    return "deleted"


class _UploadStream:
    """File-like wrapper around a streaming GET response.

    Giving requests an object with __len__ makes it send a proper
    Content-Length header WITHOUT chunked Transfer-Encoding.
    (Generator + manual Content-Length sends BOTH -> Apache 400.)
    """

    def __init__(self, resp, progress: dict, cancel: threading.Event | None = None):
        self.resp = resp
        self.progress = progress
        self.cancel = cancel
        self._it = resp.iter_content(chunk_size=CHUNK)
        self.done = 0
        # Seconds spent blocked waiting on the source. Subtracted from the PUT
        # wall time to isolate the socket-write cost -- see fmt_timing.
        self.timing = {"read_s": 0.0}

    def __len__(self):
        return int(self.resp.headers.get("Content-Length") or 0)

    def read(self, size=-1):  # noqa: ANN001 - requests calls read(chunk_size)
        if self.cancel is not None and self.cancel.is_set():
            raise UploadCancelled()
        t0 = time.monotonic()
        try:
            chunk = next(self._it)
        except StopIteration:
            chunk = None
        self.timing["read_s"] += time.monotonic() - t0
        if not chunk:
            return b""
        self.done += len(chunk)
        self.progress["done"] = self.done
        _BANDWIDTH.consume(len(chunk))
        return chunk


class _BandwidthCap:
    """Global upload rate cap in bytes/sec. A bps of 0 disables it entirely.

    Virtual-scheduling limiter: each caller claims a slot due no earlier than
    the previous one's deadline plus the time its bytes are entitled to, then
    sleeps until that slot. Uploads run on worker threads, so the claim is made
    under a lock but the sleep happens outside it, otherwise one capped upload
    would freeze the others while it waits.
    """

    def __init__(self, bps: float):
        self.bps = bps
        self._lock = threading.Lock()
        self._due = 0.0

    def consume(self, n: int) -> None:
        if self.bps <= 0 or n <= 0:
            return
        with self._lock:
            now = time.monotonic()
            due = max(now, self._due)
            self._due = due + n / self.bps
        if due > now:
            time.sleep(due - now)


_BANDWIDTH = _BandwidthCap(MAX_UPLOAD_SPEED_MBS * 1024 * 1024)


def fmt_size(n: int) -> str:
    if not n:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def fmt_size_si(n: int) -> str:
    """Decimal sizes, so they line up with Telegram's own '20 MB' wording.

    fmt_size is binary (1 MB = 1048576), which would render the 20000000-byte
    Telegram cap as "19.1 MB" -- smaller than the limit, next to a file that was
    just rejected for being over it.
    """
    if not n:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1000:
            return f"{n:.1f} {unit}"
        n /= 1000
    return f"{n:.1f} PB"


def fmt_bar(done: int, total: int, width: int = 12) -> str:
    if not total:
        mb = done / 1048576
        return f"⬆️ {mb:.1f} MB uploaded"
    pct = min(100, done * 100 // total)
    filled = pct * width // 100
    return f"[{'█' * filled}{'░' * (width - filled)}] {pct}% ({fmt_size(done)}/{fmt_size(total)})"


def fmt_timing(size: int, timing: dict) -> str:
    """Say where the transfer time went: waiting on the source, or on teracloud.

    The two stages are strictly serial today (http.client.send() alternates
    read/sendall), so they add up. Pipelining them would give the faster stage,
    i.e. min(read_s, write_s) worth of one megabyte -- hence the factor printed
    at the end. A factor near 1.0 means rewriting the transfer loop would win
    nothing; a large one means overlapping download and upload is worth doing.
    """
    rs = float(timing.get("read_s") or 0.0)
    ws = float(timing.get("write_s") or 0.0)
    total = rs + ws
    if size <= 0 or total <= 0:
        return ""

    def rate(secs: float) -> str:
        return f"{size / secs / 1048576:.1f} MB/s" if secs > 0 else "instant"

    bottleneck = "source" if rs >= ws else "teracloud"
    slower = min(rs, ws)
    # When one stage is effectively free the other already sets the pace, so
    # overlapping buys nothing -- report 1.0x rather than dividing by ~0 and
    # printing a nonsense factor.
    gain = total / slower if slower >= total * 0.05 else 1.0
    return (f"source {rs:.1f}s ({rate(rs)}) | "
            f"teracloud {ws:.1f}s ({rate(ws)}) | "
            f"bottleneck={bottleneck} | pipelining would give ~{gain:.1f}x")


# ---------- webdav listing ----------

def webdav_speed_test(size_mb: int, rounds: int, budget: float) -> dict:
    """Measure upload throughput to the WebDAV host, with no source involved.

    This isolates the teracloud half of a transfer -- the half that decides how
    long a real job takes. Every round writes a throwaway file and deletes it
    again straight away, so nothing is left for /clean to trip over. Payload is
    os.urandom so an over-eager server cannot cheat by storing zeros.
    """
    sess = requests.Session()
    sess.auth = (WEBDAV_USERNAME, WEBDAV_PASSWORD)
    payload = os.urandom(size_mb * 1024 * 1024)
    out: dict = {"size_mb": size_mb, "rounds": [], "rtt_ms": None, "errors": []}
    stamp = time.strftime("%Y%m%d-%H%M%S")

    # min() over a few tiny round trips: the floor of the path latency, with
    # queueing noise from any one of them averaged away.
    rtts = []
    probe = webdav_join(WEBDAV_DIR, f".infi-speedprobe-{stamp}")
    for _ in range(3):
        t0 = time.monotonic()
        try:
            sess.head(probe, timeout=20)
        except Exception:
            break
        rtts.append(time.monotonic() - t0)
    if rtts:
        out["rtt_ms"] = round(min(rtts) * 1000)

    written = []
    spent = 0.0
    try:
        for i in range(rounds):
            url = webdav_join(WEBDAV_DIR, f".infi-speedtest-{stamp}-{i}")
            written.append(url)
            t0 = time.monotonic()
            try:
                r = sess.put(url, data=payload, timeout=(STALL_TIMEOUT, 300),
                             headers={"Content-Type": "application/octet-stream"})
                dt = time.monotonic() - t0
                r.raise_for_status()
            except Exception as e:
                # Stop, but keep whatever rounds already succeeded -- a single
                # rejected PUT (quota, 507) should not throw away a measurement
                # that is otherwise perfectly usable.
                out["errors"].append(f"{type(e).__name__}: {e}")
                break
            out["rounds"].append({"mb": size_mb, "seconds": dt})
            spent += dt
            if spent >= budget:
                break
    finally:
        # Covers a half-written file too: these are ours and throwaway.
        for u in written:
            delete_quietly(sess, u)
        sess.close()
    return out


def fmt_speed_test(res: dict) -> str:
    """Report the spread, not just one number.

    Two rounds of the same payload can differ by 2x on a lossy high-latency
    path, and averaging that away hides the thing worth knowing: that the link
    is dropping packets and the congestion window is collapsing between bursts.
    """
    rounds = res["rounds"]
    if not rounds:
        return "❌ No successful round -- check the WebDAV URL/credentials in .env."
    speeds = sorted(r["mb"] / r["seconds"] for r in rounds)
    best, worst = speeds[-1], speeds[0]
    n = len(speeds)
    # True median, averaging the middle pair on an even count -- picking the
    # upper one would report "typical" as the best result and hide the spread.
    mid = speeds[n // 2] if n % 2 else (speeds[n // 2 - 1] + speeds[n // 2]) / 2
    gib = 1024 ** 3
    mins = gib / (best * 1048576) / 60
    est = f"{mins:.0f} min" if mins < 90 else f"{mins / 60:.1f} h"

    lines = ["\U0001f680 InfiCloud upload speed", ""]
    for i, r in enumerate(rounds, 1):
        lines.append(f"Round {i}: {r['mb']:>4} MB in "
                     f"{r['seconds']:6.1f}s  =  {r['mb'] / r['seconds']:.2f} MB/s")
    lines += ["",
              f"Best:    {best:.2f} MB/s",
              f"Typical: {mid:.2f} MB/s   (median)",
              f"Worst:   {worst:.2f} MB/s"]
    if res.get("rtt_ms"):
        lines.append(f"Round trip: {res['rtt_ms']} ms")

    notes = []
    ratio = best / worst if worst > 0 else 1.0
    if len(speeds) > 1 and ratio >= 1.5:
        notes.append(f"\U0001f534 Unsteady: rounds differ by {ratio:.1f}x. That is "
                     "packet loss, not bandwidth — TCP's congestion window is "
                     "collapsing and re-growing between bursts, so treat the "
                     "figure as a range.")
    if res.get("rtt_ms", 0) >= 120:
        notes.append(f"\U0001f7e1 Path latency is {res['rtt_ms']} ms, so TCP needs "
                     "several round trips just to fill its window. A small "
                     "sample measures that ramp more than the line itself — run "
                     "/speed 64 for a steady-state number.")
    if notes:
        lines += [""] + notes
    if res.get("errors"):
        lines.append(f"⚠️ a round failed — first: {res['errors'][0][:120]}")
    lines += ["", f"1 GB would take about {est} at the best rate.", ""]
    lines.append(f"Target: {WEBDAV_URL}{WEBDAV_DIR + '/' if WEBDAV_DIR else ''}")
    if MAX_UPLOAD_SPEED_MBS:
        # Would otherwise look like the cap is being ignored -- it isn't: this
        # test talks to WebDAV directly and never passes through _BandwidthCap.
        lines.append(f"(MAX_UPLOAD_SPEED_MBS={MAX_UPLOAD_SPEED_MBS:g} is set. "
                     "It does not apply here, nor to real uploads above it.)")
    return "\n".join(lines)


def list_webdav() -> list[dict]:
    """PROPFIND the target dir -> [{name, dir, size, mtime}].

    Shared by /ls and /clean so the two can never disagree about what is there.
    Blocking; call via asyncio.to_thread.
    """
    import xml.etree.ElementTree as ET

    sess = requests.Session()
    sess.auth = (WEBDAV_USERNAME, WEBDAV_PASSWORD)
    try:
        body = ('<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop>'
                '<D:displayname/><D:getcontentlength/><D:getlastmodified/>'
                '<D:resourcetype/></D:prop></D:propfind>')
        r = sess.request("PROPFIND", webdav_join(WEBDAV_DIR), data=body.encode(),
                         headers={"Depth": "1", "Content-Type": "application/xml"},
                         timeout=25)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        ns = {"D": "DAV:"}
        base = WEBDAV_DIR.split("/")[-1] if WEBDAV_DIR else "dav"
        out: list[dict] = []
        for resp in root.findall("D:response", ns):
            href = (resp.findtext("D:href", "", ns) or "").strip()
            is_dir = resp.find("D:propstat/D:prop/D:resourcetype/D:collection", ns) is not None
            name = urllib.parse.unquote(href.rstrip("/").split("/")[-1])
            if not name or name in ("dav", base):
                continue
            entry = {"name": name, "dir": is_dir, "size": 0, "mtime": None}
            if not is_dir:
                ln = (resp.findtext("D:propstat/D:prop/D:getcontentlength", "", ns) or "0").strip()
                entry["size"] = int(ln) if ln.isdigit() else 0
                lm = (resp.findtext("D:propstat/D:prop/D:getlastmodified", "", ns) or "").strip()
                if lm:
                    try:
                        entry["mtime"] = parsedate_to_datetime(lm).timestamp()
                    except (TypeError, ValueError):
                        entry["mtime"] = None
            out.append(entry)
        return out
    finally:
        sess.close()


# ---------- telegram handlers ----------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text(
        "👋 Send me a direct download link and I'll upload it straight to InfiniCloud WebDAV.\n\n"
        "• Just paste one or more http(s) links — up to 10, "
        f"{MAX_CONCURRENT} upload in parallel\n"
        "• Or forward files — up to 20 MB, Telegram's cap on what a bot can download\n"
        "• /ls — list files in your WebDAV folder\n"
        "• /speed — test your upload speed to InfiCloud (/speed 32 for a bigger run)\n"
        "• /jobs — see uploads in progress\n"
        "• /cancel — stop one (/cancel <id>) or all (/cancel all)\n"
        "• /clean — find orphan/partial files (add: orphans, log, all)\n\n"
        f"📁 Target: {WEBDAV_URL}{WEBDAV_DIR + '/' if WEBDAV_DIR else ''}"
    )


async def cmd_ls(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("🔍 Listing WebDAV…")
    try:
        entries = await asyncio.to_thread(list_webdav)
    except Exception as e:
        await update.message.reply_text(f"❌ List failed: {e}")
        return
    try:
        lines = [
            f"• {e['name']} ({'dir' if e['dir'] else fmt_size(e['size'])})"
            for e in entries[:30]
        ]
        text = "📁 " + (WEBDAV_DIR or "/") + "\n" + ("\n".join(lines) if lines else "(empty)")
        await update.message.reply_text(text[:4000])
    except Exception:
        await update.message.reply_text("✅ Got listing (format failed). Check WebDAV in browser.")


async def cmd_speed(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/speed [MB] -- how fast the bot can push to InfiniCloud, right now."""
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    # /speed 32 for a bigger sample; clamped so it can't turn into a giant upload.
    size_mb = 8
    if ctx.args:
        try:
            size_mb = int(ctx.args[0])
        except ValueError:
            await update.message.reply_text("Usage: /speed [MB]  (e.g. /speed 32)")
            return
        if not 1 <= size_mb <= 512:
            await update.message.reply_text("Please choose between 1 and 512 MB.")
            return
    busy = len(JOBS)
    msg = await update.message.reply_text(
        f"🚀 Testing upload to InfiniCloud ({size_mb} MB per round)…")
    try:
        res = await asyncio.to_thread(webdav_speed_test, size_mb, 5, 20.0)
    except Exception as e:
        await update.message.reply_text(f"❌ Speed test failed: {e}")
        return
    text = fmt_speed_test(res)
    if busy:
        text += f"\n\n⚠️ {busy} upload(s) were running during this test and were " \
                "sharing the same line — the numbers are a floor, not your ceiling."
    try:
        await msg.edit_text(text)
    except Exception:
        await update.message.reply_text(text)


async def handle_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    text = update.message.text or ""
    urls = URL_RE.findall(text)
    if not urls:
        await update.message.reply_text("Send a direct http(s) download link. /help for usage.")
        return
    urls = [u.strip().rstrip(").,!?") for u in urls[:MAX_LINKS_PER_MSG]]
    # fan out: each link becomes its own job, bounded by the global semaphore.
    # Detached + immediate return keeps /jobs and /cancel responsive.
    for url in urls:
        spawn(process_one_url(update, ctx, url))
    note = (f"\n⚠️ Extra links beyond {MAX_LINKS_PER_MSG} were ignored."
            if len(URL_RE.findall(text)) > MAX_LINKS_PER_MSG else "")
    await update.message.reply_text(
        f"📥 Queued {len(urls)} link(s) as job(s).\n/jobs to watch · /cancel all to stop.{note}"
    )


def tg_oversize_message(size: int) -> str:
    """Explain the Telegram file cap instead of leaking a bare 400 at the user."""
    what = fmt_size_si(size) if size > 0 else "an unknown size"
    return (
        f"\U0001f4e6 That file is {what}, but Telegram only lets bots download "
        f"files up to {fmt_size_si(TG_FILE_MAX_BYTES)}.\n"
        "It's a Telegram limit rather than a problem here — forwarding it "
        "another way won't help.\n\n"
        "Send a direct http(s) download link instead. Links have no size limit "
        "(a multi-GB one has uploaded fine)."
    )


def is_too_big_error(e: Exception) -> bool:
    """True when Telegram refused getFile because the file exceeds its cap.

    Matched on wording rather than on telegram.error.BadRequest: this try block
    only ever calls get_file(), so the phrase is unambiguous, and it keeps
    working if the exception type or message wording shifts between API versions.
    """
    msg = str(e).lower()
    return "too big" in msg or "too large" in msg


async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Also accept files sent directly to the bot (Telegram file -> WebDAV)."""
    if not is_allowed(update):
        return
    doc = update.message.document or update.message.video or update.message.audio or update.message.photo
    # NOTE: Message.photo defaults to an empty tuple (), not None. Anything that
    # matches filters.ATTACHMENT but isn't a document/video/audio/photo (sticker,
    # voice, animation, video note) falls through to () here, so a `doc is None`
    # check is not enough -> doc[-1] used to raise IndexError on those.
    if not doc:
        return
    if isinstance(doc, (list, tuple)):  # photo returns list of sizes
        doc = doc[-1]
    size = getattr(doc, "file_size", 0) or 0
    # The update carries the size even though the bytes are unreachable, so say
    # so up front instead of letting get_file() fail with an opaque error.
    if size > TG_FILE_MAX_BYTES:
        await update.message.reply_text(tg_oversize_message(size))
        return
    try:
        tg_file = await doc.get_file()
        file_url = tg_file.file_path  # direct https URL (includes token)
        if file_url and not file_url.startswith("http"):
            file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_url}"
        # detached, same reason as handle_link
        spawn(process_one_url(update, ctx, file_url, hint_name=getattr(doc, "file_name", None)))
        await update.message.reply_text("📥 Queued. /jobs to watch, /cancel all to stop.")
    except Exception as e:
        # Belt and braces: the cap is enforced server-side and may not match the
        # constant above exactly, and file_size can be absent. Let Telegram's
        # own verdict win rather than second-guessing it.
        if is_too_big_error(e):
            await update.message.reply_text(tg_oversize_message(size))
            return
        await update.message.reply_text(f"❌ Could not read Telegram file: {e}")


async def process_one_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url: str, hint_name: str | None = None):
    user_id = update.effective_user.id if update.effective_user else 0
    job = new_job(user_id, url, hint_name or url.split("/")[-1][:60])
    status = await update.message.reply_text(f"⏳ Queued as job #{job['id']}…\n{url[:200]}")
    progress = job["progress"]
    progress["_job"] = job  # lets the worker thread emit its own audit records
    last_edit = [0.0]

    async def poll():
        while not progress.get("finished"):
            await asyncio.sleep(3)
            now = time.time()
            if now - last_edit[0] < 2.5:
                continue
            last_edit[0] = now
            try:
                bar = fmt_bar(progress.get("done", 0), progress.get("total", 0))
                await status.edit_text(
                    f"⬆️ Job #{job['id']} — {progress.get('stage', '')}\n"
                    f"📄 {progress.get('filename', '')[:80]}\n{bar}\n"
                    f"❌ /cancel {job['id']}"
                )
            except Exception:
                pass  # message not modified etc.

    sem = JOB_SEM or asyncio.Semaphore(MAX_CONCURRENT)
    async with sem:
        if job["cancel"].is_set():
            finish_job(job, error="cancelled")
            record_event(job, "cancelled", seconds=time.time() - job["started"])
            try:
                await status.edit_text(f"🚫 Job #{job['id']} cancelled before it started.")
            except Exception:
                pass
            return
        poller = asyncio.create_task(poll())
        t0 = time.time()
        try:
            result = await asyncio.to_thread(do_stream_upload, url, progress, job["cancel"])
            progress["finished"] = True
            finish_job(job, result=result)
            try:
                await poller
            except Exception:
                pass
            dt = time.time() - t0
            speed = result["size"] / dt / 1048576 if dt > 0 and result["size"] else 0
            record_event(job, "done", filename=result["filename"],
                         seconds=dt, size=result["size"],
                         read_s=result.get("timing", {}).get("read_s"),
                         write_s=result.get("timing", {}).get("write_s"))
            log.info("job #%s done: %s (%s in %.0fs, %.1f MB/s)", job["id"],
                     result["filename"], fmt_size(result["size"]), dt, speed)
            detail = fmt_timing(result["size"], result.get("timing") or {})
            if detail:
                log.info("job #%s timing: %s", job["id"], detail)
            await status.edit_text(
                f"✅ Uploaded to InfiniCloud! (job #{job['id']})\n"
                f"📄 {result['filename']}\n"
                f"📦 {fmt_size(result['size'])} in {dt:.0f}s ({speed:.1f} MB/s)\n"
                f"📁 {result['webdav_path']}"
            )
        except UploadCancelled as e:
            progress["finished"] = True
            finish_job(job, error="cancelled")
            try:
                await poller
            except Exception:
                pass
            record_event(job, "cancelled", seconds=time.time() - job["started"])
            log.info("job #%s cancelled by user — %s discarded",
                     job["id"], fmt_size(progress.get("done", 0)))
            try:
                await status.edit_text(
                    f"🚫 Job #{job['id']} cancelled — partial file removed.\n"
                    f"📄 {progress.get('filename', '')[:80]}\n"
                    f"📦 {fmt_size(progress.get('done', 0))} discarded"
                )
            except Exception:
                pass
        except Exception as e:
            progress["finished"] = True
            finish_job(job, error=str(e))
            try:
                await poller
            except Exception:
                pass
            record_event(job, "failed", seconds=time.time() - job["started"], error=str(e)[:400])
            log.exception("job #%s failed after %.0fs: %s", job["id"],
                          time.time() - job["started"], str(e)[:200])
            try:
                await status.edit_text(f"❌ Failed (job #{job['id']}):\n{url[:200]}\n\n{str(e)[:800]}")
            except Exception:
                pass
        finally:
            if not poller.done():
                progress["finished"] = True
                poller.cancel()


# ---------- job listing / cancel / clean ----------

ICON = {"done": "✅", "failed": "❌", "cancelled": "🚫", "queued": "⏳",
        "started": "⬆️", "cleaned": "🧹"}


def history_line(rec: dict) -> str:
    ev = rec.get("event", "?")
    name = (rec.get("filename") or "?")[:38]
    secs = rec.get("seconds") or 0
    detail = ""
    if ev == "done":
        detail = f"{fmt_size(rec.get('done', 0))} in {secs:.0f}s"
    elif ev == "cancelled":
        detail = f"{fmt_size(rec.get('done', 0))} discarded after {secs:.0f}s"
    elif ev == "failed":
        detail = f"after {secs:.0f}s — {(rec.get('error') or '')[:90]}"
    return (f"{ICON.get(ev, '•')} {rec.get('ts', '')} · #{rec.get('job')} "
            f"{name}\n   {detail}")


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    user_id = update.effective_user.id if update.effective_user else 0
    args = ctx.args or []
    limit = int(args[0]) if args and args[0].isdigit() else 20
    limit = max(1, min(limit, 50))
    # One read serves both the listing and the tally. They used to be two
    # full-file parses of jobs.jsonl, back to back, on the event loop.
    recs = await asyncio.to_thread(read_history, user_id, max(limit, 200))
    if not recs:
        await update.message.reply_text("📭 No job history recorded yet.")
        return
    body = "\n".join(history_line(r) for r in reversed(recs[:limit]))
    counts: dict[str, int] = {}
    for r in recs[:200]:
        counts[r.get("event", "?")] = counts.get(r.get("event", "?"), 0) + 1
    tally = " · ".join(f"{ICON.get(k, '•')} {counts[k]}" for k in
                       ("done", "failed", "cancelled") if counts.get(k))
    await update.message.reply_text(f"🗂 Last {len(recs)} jobs\n{tally}\n\n{body}"[:4000])


SUFFIXED_RE = re.compile(r"_\d+(?:\.[A-Za-z0-9]{1,5})?$")


def index_records(records: list[dict]) -> dict[str, dict]:
    """Latest audit record per dest filename."""
    by_name: dict[str, dict] = {}
    for rec in records:
        name = (rec.get("filename") or "").strip()
        if not name:
            continue
        prev = by_name.get(name)
        if prev is None or (rec.get("job") or 0) >= (prev.get("job") or 0):
            by_name[name] = rec
    return by_name


def find_orphans(entries: list[dict], records: list[dict],
                 now: float | None = None) -> list[tuple[dict, str]]:
    """Classify remote entries as deletable. Returns [(entry, reason)].

    STRONG = we have proof: a non-terminal audit record for that exact name whose
    recorded total disagrees with the remote size (or is zero bytes).
    WEAK   = circumstantial: tiny _N duplicate or long-dead zero-byte file with
             no evidence of a successful upload. Gated behind CLEAN_WEAK and an
             age threshold, and never applied to a file younger than
             CLEAN_MIN_FREE_SECS, which could still be in flight.
    """
    now = time.time() if now is None else now
    by_name = index_records(records)
    age_cutoff = now - CLEAN_MAX_AGE_HOURS * 3600
    out: list[tuple[dict, str]] = []

    for e in entries:
        if e["dir"]:
            continue
        name, size, mtime = e["name"], e["size"], e["mtime"]
        if mtime is not None and (now - mtime) < CLEAN_MIN_FREE_SECS:
            continue  # too fresh to be an orphan; may be a live upload
        rec = by_name.get(name)

        if rec and rec.get("event") in ("started", "failed", "cancelled"):
            expected = int(rec.get("total") or 0)
            if size == 0:
                out.append((e, f"strong: job #{rec.get('job')} {rec.get('event')}, zero bytes"))
                continue
            if expected > 0 and size != expected:
                out.append((e, f"strong: job #{rec.get('job')} {rec.get('event')}, "
                                 f"{size} of {expected} bytes"))
                continue

        if rec and rec.get("event") == "done":
            continue  # confirmed good upload

        if not CLEAN_WEAK or mtime is None or mtime > age_cutoff:
            continue

        if size == 0 and rec is None:
            out.append((e, "weak: zero-byte, no job record"))
            continue
        if size < ORPHAN_TINY_BYTES and SUFFIXED_RE.search(name) and rec is None:
            out.append((e, "weak: tiny _N duplicate, no job record"))
            continue
    return out


async def cmd_clean(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """/clean = report only. /clean orphans | log | all actually delete."""
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    args = [a.strip().lower() for a in (ctx.args or [])]
    mode = args[0] if args else "report"
    if mode not in ("report", "orphans", "log", "all"):
        await update.message.reply_text(
            "Usage:\n"
            "  /clean — report orphan/partial files (deletes nothing)\n"
            "  /clean orphans — delete them\n"
            "  /clean log — rotate log, compact jobs.jsonl\n"
            "  /clean all — both"
        )
        return

    do_delete = mode in ("orphans", "all")
    summary = ""

    if mode in ("orphans", "all", "report"):
        await update.message.reply_text("🔍 Scanning WebDAV…")
        try:
            entries = await asyncio.to_thread(list_webdav)
        except Exception as e:
            await update.message.reply_text(f"❌ Could not list WebDAV: {e}")
            return
        # Every user's records: an orphan may predate this bot's job numbering.
        # Reading the log and classifying against up to JOB_LOG_KEEP records is
        # far too slow to sit on the event loop, so it runs in a worker.
        def _find_orphans():
            return find_orphans(entries, read_history(None, JOB_LOG_KEEP))

        orphans = await asyncio.to_thread(_find_orphans)
        files = [e for e in entries if not e["dir"]]
        if not orphans:
            summary += (f"🧹 Clean: {len(files)} file(s) scanned, "
                        f"0 orphan(s), nothing to remove.\n")
        else:
            lines = [f"• {e['name'][:44]} ({fmt_size(e['size'])}) — {why}"
                     for e, why in orphans[:25]]
            more = f"\n… and {len(orphans) - 25} more" if len(orphans) > 25 else ""
            if not do_delete:
                summary += (f"🧹 {len(orphans)} orphan(s) of {len(files)} file(s):\n"
                            + "\n".join(lines) + more
                            + "\n\nDry run — nothing deleted. Use /clean orphans.")
            else:
                def _delete():
                    sess = requests.Session()
                    sess.auth = (WEBDAV_USERNAME, WEBDAV_PASSWORD)
                    gone, failed = 0, 0
                    try:
                        for e, _why in orphans[:CLEAN_MAX_DELETES]:
                            try:
                                r = sess.delete(webdav_join(WEBDAV_DIR, e["name"]), timeout=30)
                                if r.status_code in (200, 204, 404):
                                    gone += 1
                                else:
                                    failed += 1
                            except Exception:
                                failed += 1
                    finally:
                        sess.close()
                    return gone, failed

                gone, failed = await asyncio.to_thread(_delete)
                for e, why in orphans[:gone]:
                    log.info("clean: deleted %s (%s)", e["name"], why)
                skipped = max(0, len(orphans) - CLEAN_MAX_DELETES)
                summary += (f"🧹 Deleted {gone} orphan(s)."
                            + (f" {failed} failed." if failed else "")
                            + (f" {skipped} skipped (CLEAN_MAX_DELETES)."
                               if skipped else "") + "\n")
                if gone:
                    summary += "\n".join(lines[:10]) + "\n"
                if not do_delete:
                    summary += "\nDry run — nothing deleted."

    if mode in ("log", "all"):
        before, after = compact_job_log(max_bytes=JOB_LOG_MAX_BYTES)
        log_size = 0
        try:
            log_size = os.path.getsize(LOG_PATH) if os.path.exists(LOG_PATH) else 0
        except OSError:
            pass
        summary += (f"📄 jobs.jsonl: {before} -> {after} record(s) kept. "
                    f"bot log {fmt_size(log_size)}"
                    f" (rotates automatically at {fmt_size(LOG_MAX_BYTES)}).\n")

    await update.message.reply_text(summary[:4000])


def job_line(job: dict) -> str:
    p = job["progress"]
    state = p.get("stage", "?")
    bar = fmt_bar(p.get("done", 0), p.get("total", 0), width=10)
    mins = (time.time() - job["started"]) / 60
    name = p.get("filename", "?")[:40]
    return f"#{job['id']} [{state}] {name}\n     {bar} · {mins:.0f}m"


async def cmd_jobs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    user_id = update.effective_user.id if update.effective_user else 0
    mine = jobs_for_user(user_id)
    recent = recent_for_user(user_id)
    if not mine and not recent:
        await update.message.reply_text("📭 No uploads in progress.")
        return
    if not mine:
        await update.message.reply_text("📭 No uploads in progress.\n\nRecent:")
        for j in recent:
            await update.message.reply_text(job_outcome(j))
        return

    header = (f"📶 {len(mine)} upload(s) in progress "
              f"(max {MAX_CONCURRENT} concurrent)\n")
    body = "\n".join(job_line(j) for j in mine)
    text = f"{header}\n{body}"
    await update.message.reply_text(text[:4000])
    if recent:
        tail = "\n".join(job_outcome(j) for j in recent)
        await update.message.reply_text("Recent:\n" + tail)


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """`/cancel` cancels your newest upload; `/cancel <id>` a specific one;
    `/cancel all` cancels every upload of yours."""
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    user_id = update.effective_user.id if update.effective_user else 0
    mine = jobs_for_user(user_id)
    if not mine:
        await update.message.reply_text("📭 Nothing to cancel.")
        return

    args = ctx.args or []
    if args and args[0].lower() == "all":
        targets = mine
    elif args:
        if not args[0].isdigit():
            await update.message.reply_text("Usage: /cancel [job_id|all]")
            return
        job = get_job(int(args[0]))
        if job is None or job["user_id"] != user_id:
            await update.message.reply_text(f"❌ No active job #{args[0]} of yours.")
            return
        targets = [job]
    else:
        targets = [mine[-1]]  # newest

    for job in targets:
        request_cancel(job)
    names = ", ".join(f"#{j['id']}" for j in targets)
    await update.message.reply_text(
        f"🚫 Cancelling {names}… (stops at the next 256 KB chunk, "
        f"partial file gets deleted)"
    )


def main():
    ap = argparse.ArgumentParser(description="Telegram -> InfiniCloud WebDAV relay")
    ap.add_argument("--log", default=None,
                    help="write logs to this rotating file instead of stdout "
                         f"(default {LOG_PATH})")
    args = ap.parse_args()
    if args.log:
        setup_file_log(args.log)

    if not BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN in .env")
    if not WEBDAV_URL or not WEBDAV_USERNAME or not WEBDAV_PASSWORD:
        raise SystemExit("Missing WEBDAV_URL / WEBDAV_USERNAME / WEBDAV_PASSWORD in .env")

    async def post_init(app: Application):
        global JOB_SEM
        JOB_SEM = asyncio.Semaphore(MAX_CONCURRENT)
        # Registers commands so Telegram shows them in the "/" menu. Purely
        # cosmetic, and it used to be the one call that could kill startup: a
        # timed-out set_my_commands took the whole bot down with it. Losing the
        # menu must never stop the bot from serving uploads.
        try:
            await app.bot.set_my_commands([
                BotCommand("start", "Start the bot / show help"),
                BotCommand("help", "How to use the bot"),
                BotCommand("ls", "List files in WebDAV folder"),
                BotCommand("speed", "Test upload speed to InfiCloud: /speed [MB]"),
                BotCommand("jobs", "Show uploads in progress"),
                BotCommand("history", "Past jobs, incl. failures"),
                BotCommand("cancel", "Cancel an upload: /cancel [id|all]"),
                BotCommand("clean", "Find orphan/partial files, tidy logs"),
            ])
        except Exception as e:
            log.warning("could not register the command menu "
                        "(cosmetic only, the bot still works): %s", e)

    async def post_shutdown(app: Application):
        # Ask every in-flight upload to stop so nothing is left half-written.
        for job in list(JOBS.values()):
            request_cancel(job)
        log.info("shutdown: cancelling %d in-flight upload(s)", len(BACKGROUND))

    async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
        """Two bot instances polling the same token -> Conflict every poll cycle.

        That means neither instance can serve commands, so say it once, loudly,
        instead of dumping a traceback every 35 seconds.
        """
        err = context.error
        if isinstance(err, Conflict):
            log.error(
                "Conflict: another bot instance is polling this token. "
                "This instance will NOT receive updates. "
                "Find it with: pgrep -af bot.py"
            )
            return
        log.error("update error: %s", err, exc_info=err)


    # python-telegram-bot defaults every Telegram timeout to 5s and the pool
    # wait to 1s. On a VPN, a mobile link or a briefly stuttering tunnel that is
    # easily exceeded, and the consequence was a hard crash at startup. These are
    # generous on purpose: a Telegram API call taking 30s is slow, not broken.
    # read_timeout must comfortably exceed the long-poll window or every getUpdates
    # would time out. 64 connections is far above the ~3+1 this bot needs.
    tg_request = HTTPXRequest(
        connection_pool_size=64,
        connect_timeout=TG_CONNECT_TIMEOUT,
        read_timeout=TG_READ_TIMEOUT,
        write_timeout=TG_CONNECT_TIMEOUT,
        pool_timeout=TG_POOL_TIMEOUT,
    )

    app = Application.builder().token(BOT_TOKEN).request(tg_request).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("ls", cmd_ls))
    app.add_handler(CommandHandler("speed", cmd_speed))
    app.add_handler(CommandHandler("jobs", cmd_jobs))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("clean", cmd_clean))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_handler(MessageHandler(filters.ATTACHMENT, handle_file))
    app.add_error_handler(on_error)

    log.info("Bot starting. WebDAV target: %s%s", WEBDAV_URL, WEBDAV_DIR + "/" if WEBDAV_DIR else "")
    # Uploads run as detached tasks (see spawn()), so the sequential update
    # loop stays free to answer /jobs and /cancel during a long transfer.
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
