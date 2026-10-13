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

import asyncio
import logging
import os
import re
import time
import urllib.parse
from pathlib import PurePosixPath

import requests
from dotenv import load_dotenv
from telegram import BotCommand, Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

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


def browser_headers(url: str) -> dict:
    parsed = urllib.parse.urlparse(url)
    referer = f"{parsed.scheme}://{parsed.netloc}/"
    return {
        "User-Agent": BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Upgrade-Insecure-Requests": "1",
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


def ensure_webdav_dir(session: requests.Session, dest_dir_url: str):
    """MKCOL each level of WEBDAV_DIR. Ignore 405/201/200 (already exists)."""
    if not WEBDAV_DIR:
        return
    accum = WEBDAV_URL
    for part in WEBDAV_DIR.split("/"):
        part = part.strip()
        if not part:
            continue
        accum = accum + urllib.parse.quote(part, safe="") + "/"
        try:
            r = session.request("MKCOL", accum, timeout=20)
            if r.status_code in (201, 200, 405, 409):
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


def do_stream_upload(source_url: str, progress: dict) -> dict:
    """Blocking: GET source_url (stream) -> PUT to WebDAV (stream). No disk I/O.

    progress dict is mutated: {done, total, filename, stage}
    Returns {filename, size, webdav_url, webdav_path}
    Raises on HTTP error.
    """
    dl = requests.Session()  # no auth! never send WebDAV creds to 3rd-party hosts
    dav = requests.Session()
    dav.auth = (WEBDAV_USERNAME, WEBDAV_PASSWORD)

    ensure_webdav_dir(dav, WEBDAV_URL)

    progress["stage"] = "connecting"
    headers = browser_headers(source_url)
    try:
        src_resp = dl.get(source_url, stream=True, timeout=(15, 60),
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
                return do_stream_upload(direct, progress)
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
        # avoid overwrite: if file exists, append _1, _2 ...
        # cheap check with HEAD (InfiniCloud supports HEAD per spec)
        stem, dot, suffix = fname.rpartition(".")
        counter = 0
        probe_name = fname
        while True:
            try:
                h = dav.head(webdav_join(WEBDAV_DIR, probe_name), timeout=15)
                if h.status_code == 404:
                    fname = probe_name
                    dest_url = webdav_join(WEBDAV_DIR, probe_name)
                    break
                if h.status_code in (200, 301, 302):
                    counter += 1
                    probe_name = f"{stem}_{counter}.{suffix}" if dot else f"{fname}_{counter}"
                    if counter > 100:
                        break
                    continue
                break  # on auth error etc, don't loop
            except Exception:
                break

        progress.update(filename=fname, total=total, done=0, stage="uploading")
        log.info("PUT %s | size=%s type=%s (src=%s)",
                 dest_url, fmt_size(total) if total else "unknown", ctype, src.url)

        put_headers = {"Content-Type": ctype or "application/octet-stream"}

        progress["stage"] = "uploading"
        if total > 0:
            # Sized stream -> requests sends Content-Length, no chunked encoding.
            # (Old code used a generator + manual Content-Length which sends
            #  BOTH Content-Length and Transfer-Encoding: chunked -> Apache 400.)
            stream = _UploadStream(src, progress)
            put = dav.put(dest_url, data=stream, headers=put_headers,
                          timeout=(15, 60 * 60 * 6))
            downloaded = stream.done
        else:
            # Unknown size -> true chunked upload
            downloaded = 0

            def gen():
                nonlocal downloaded
                for chunk in src.iter_content(chunk_size=CHUNK):
                    if not chunk:
                        continue
                    downloaded += len(chunk)
                    progress["done"] = downloaded
                    yield chunk

            put = dav.put(dest_url, data=gen(), headers=put_headers,
                          timeout=(15, 60 * 60 * 6))
        try:
            put.raise_for_status()
        except requests.HTTPError as e:
            log.error("PUT failed %s dest=%s req-headers=%s resp-headers=%s body=%s",
                      put.status_code, dest_url,
                      dict(put.request.headers), dict(put.headers), put.text[:500])
            raise RuntimeError(f"WebDAV PUT failed ({put.status_code}): {put.text[:300]}") from e

        webdav_path = f"{WEBDAV_DIR}/{fname}" if WEBDAV_DIR else fname
        return {"filename": fname, "size": downloaded, "total": total,
                "webdav_url": dest_url, "webdav_path": webdav_path}


class _UploadStream:
    """File-like wrapper around a streaming GET response.

    Giving requests an object with __len__ makes it send a proper
    Content-Length header WITHOUT chunked Transfer-Encoding.
    (Generator + manual Content-Length sends BOTH -> Apache 400.)
    """

    def __init__(self, resp, progress: dict):
        self.resp = resp
        self.progress = progress
        self._it = resp.iter_content(chunk_size=CHUNK)
        self.done = 0

    def __len__(self):
        return int(self.resp.headers.get("Content-Length") or 0)

    def read(self, size=-1):  # noqa: ANN001 - requests calls read(chunk_size)
        try:
            chunk = next(self._it)
        except StopIteration:
            return b""
        if not chunk:
            return b""
        self.done += len(chunk)
        self.progress["done"] = self.done
        return chunk


def fmt_size(n: int) -> str:
    if not n:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def fmt_bar(done: int, total: int, width: int = 12) -> str:
    if not total:
        mb = done / 1048576
        return f"⬆️ {mb:.1f} MB uploaded"
    pct = min(100, done * 100 // total)
    filled = pct * width // 100
    return f"[{'█' * filled}{'░' * (width - filled)}] {pct}% ({fmt_size(done)}/{fmt_size(total)})"


# ---------- telegram handlers ----------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text(
        "👋 Send me a direct download link and I'll upload it straight to InfiniCloud WebDAV.\n\n"
        "• Just paste the http(s) link\n"
        "• Or forward a file (small files only, Telegram limit)\n"
        "• /ls — list files in your WebDAV folder\n\n"
        f"📁 Target: {WEBDAV_URL}{WEBDAV_DIR + '/' if WEBDAV_DIR else ''}"
    )


async def cmd_ls(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    await update.message.reply_text("🔍 Listing WebDAV…")

    def _list():
        sess = requests.Session()
        sess.auth = (WEBDAV_USERNAME, WEBDAV_PASSWORD)
        url = webdav_join(WEBDAV_DIR)
        body = '<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:displayname/><D:getcontentlength/><D:resourcetype/></D:prop></D:propfind>'
        r = sess.request("PROPFIND", url, data=body.encode(),
                         headers={"Depth": "1", "Content-Type": "application/xml"}, timeout=25)
        r.raise_for_status()
        return r.text

    try:
        xml = await asyncio.to_thread(_list)
    except Exception as e:
        await update.message.reply_text(f"❌ List failed: {e}")
        return

    import xml.etree.ElementTree as ET
    try:
        ns = {"D": "DAV:"}
        root = ET.fromstring(xml)
        lines = []
        for resp in root.findall("D:response", ns)[:30]:
            href = (resp.findtext("D:href", "", ns) or "").strip()
            name_el = resp.find("D:propstat/D:prop/D:displayname", ns)
            name = (name_el.text if name_el is not None and name_el.text else urllib.parse.unquote(href.rstrip("/").split("/")[-1]))
            if not name or name in ("dav", WEBDAV_DIR.split("/")[-1] if WEBDAV_DIR else "dav"):
                continue
            length = resp.findtext("D:propstat/D:prop/D:getcontentlength", "", ns) or ""
            size = fmt_size(int(length)) if length.strip().isdigit() else "dir"
            lines.append(f"• {name} ({size})")
        text = "📁 " + (WEBDAV_DIR or "/") + "\n" + ("\n".join(lines) if lines else "(empty)")
        await update.message.reply_text(text[:4000])
    except Exception:
        await update.message.reply_text("✅ Got listing (raw parse failed). Check WebDAV in browser.")


async def handle_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text("⛔ Not authorized.")
        return
    text = update.message.text or ""
    urls = URL_RE.findall(text)
    if not urls:
        await update.message.reply_text("Send a direct http(s) download link. /help for usage.")
        return
    for url in urls[:5]:  # max 5 per message to avoid abuse
        await process_one_url(update, ctx, url.strip().rstrip(").,!?"))


async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Also accept files sent directly to the bot (Telegram file -> WebDAV)."""
    if not is_allowed(update):
        return
    doc = update.message.document or update.message.video or update.message.audio or update.message.photo
    if doc is None:
        return
    if isinstance(doc, (list, tuple)):  # photo returns list of sizes
        doc = doc[-1]
    try:
        tg_file = await doc.get_file()
        file_url = tg_file.file_path  # direct https URL (includes token)
        if file_url and not file_url.startswith("http"):
            file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_url}"
        await process_one_url(update, ctx, file_url, hint_name=getattr(doc, "file_name", None))
    except Exception as e:
        await update.message.reply_text(f"❌ Could not read Telegram file: {e}")


async def process_one_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE, url: str, hint_name: str | None = None):
    status = await update.message.reply_text(f"⏳ Starting…\n{url[:200]}")
    progress = {"done": 0, "total": 0, "filename": hint_name or url.split("/")[-1][:60], "stage": "connecting"}
    last_edit = [0.0]

    async def poll():
        while True:
            await asyncio.sleep(3)
            # stop polling once done flag set
            if progress.get("finished"):
                break
            now = time.time()
            if now - last_edit[0] < 2.5:
                continue
            last_edit[0] = now
            try:
                bar = fmt_bar(progress.get("done", 0), progress.get("total", 0))
                await status.edit_text(
                    f"⬆️ Uploading to WebDAV…\n📄 {progress.get('filename', '')[:80]}\n{bar}"
                )
            except Exception:
                pass  # message not modified etc.

    poller = asyncio.create_task(poll())
    t0 = time.time()
    try:
        result = await asyncio.to_thread(do_stream_upload, url, progress)
        progress["finished"] = True
        await poller
        dt = time.time() - t0
        speed = result["size"] / dt / 1048576 if dt > 0 and result["size"] else 0
        await status.edit_text(
            "✅ Uploaded to InfiniCloud!\n"
            f"📄 {result['filename']}\n"
            f"📦 {fmt_size(result['size'])} in {dt:.0f}s ({speed:.1f} MB/s)\n"
            f"📁 {result['webdav_path']}"
        )
    except Exception as e:
        progress["finished"] = True
        try:
            await poller
        except Exception:
            pass
        log.exception("upload failed for %s", url)
        try:
            await status.edit_text(f"❌ Failed:\n{url[:200]}\n\n{str(e)[:800]}")
        except Exception:
            pass


def main():
    if not BOT_TOKEN:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN in .env")
    if not WEBDAV_URL or not WEBDAV_USERNAME or not WEBDAV_PASSWORD:
        raise SystemExit("Missing WEBDAV_URL / WEBDAV_USERNAME / WEBDAV_PASSWORD in .env")

    async def post_init(app: Application):
        # Registers commands so Telegram shows them in the "/" menu
        await app.bot.set_my_commands([
            BotCommand("start", "Start the bot / show help"),
            BotCommand("help", "How to use the bot"),
            BotCommand("ls", "List files in WebDAV folder"),
        ])

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("ls", cmd_ls))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_handler(MessageHandler(filters.ATTACHMENT, handle_file))

    log.info("Bot starting. WebDAV target: %s%s", WEBDAV_URL, WEBDAV_DIR + "/" if WEBDAV_DIR else "")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
