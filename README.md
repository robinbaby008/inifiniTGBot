# infi-TG Bot

Telegram bot that accepts a **download link** and uploads it **directly to InfiniCloud WebDAV** — streamed chunk-by-chunk, no local disk usage.

## How it works

1. You send an `http(s)` link to the bot.
2. The bot `GET`s the file with browser-like headers and pipes it into a WebDAV `PUT` to your InfiniCloud (`https://XXXX.infini-cloud.net/dav/` or `teracloud.jp`).
3. No temp files — 256 KB chunks flow download → upload with a live progress bar in chat.

Extras:

- **Multiple files at once** — paste up to `MAX_LINKS_PER_MESSAGE` links in one message; they are queued as separate jobs and `MAX_CONCURRENT_UPLOADS` of them upload in parallel.
- **`/jobs`** — lists every upload in progress with its live progress bar, stage and elapsed time.
- **`/cancel`** — cancels your newest upload, `/cancel <id>` a specific one, `/cancel all` every one of yours. Cancellation lands at the next 256 KB chunk and aborts the PUT.
- **`/history [n]`** — every finished job, success **or** failure **or** cancel, re-read from `jobs.jsonl` on disk. Survives restarts, unlike `/jobs`.
- **`/clean`** — reports orphan/partial files left on WebDAV. **Dry run by default.** Add `orphans` to delete, `log` to compact logs, `all` for both.
- `/ls` lists files in your WebDAV folder (via `PROPFIND`).
- Files sent directly to the bot are also forwarded to WebDAV (Telegram size limits apply; links bypass that).
- Cloudflare-protected pages are detected and the bot tries a `yt-dlp` fallback to resolve the real file URL.

## Requirements

- Python 3.10+
- Telegram bot token from [@BotFather](https://t.me/BotFather)
- InfiniCloud WebDAV URL, user ID and password (WebDAV URL is shown in My Page, e.g. `https://XXXX.infini-cloud.net/dav/`)

## Setup

```bash
cp .env.example .env
# edit .env with your values (see table below)
```

| Variable | Example | Notes |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | `123456:ABC-...` | From @BotFather |
| `WEBDAV_URL` | `https://XXXX.infini-cloud.net/dav/` | Must end with `/` |
| `WEBDAV_USERNAME` | your InfiniCloud user ID | |
| `WEBDAV_PASSWORD` | your password | App password if you use one |
| `WEBDAV_DIR` | `Telegram` | Optional subfolder, empty = root |
| `ALLOWED_USER_IDS` | `123456789` | Optional, comma-separated. Empty = anyone can use the bot |
| `MAX_CONCURRENT_UPLOADS` | `3` | Optional. Links uploading in parallel. Default `3` |
| `MAX_LINKS_PER_MESSAGE` | `10` | Optional. Links taken from one message. Default `10` |
| `STALL_TIMEOUT` | `600` | Optional. Seconds of zero write progress before aborting. Default `600` |
| `SOURCE_READ_TIMEOUT` | `60` | Optional. Seconds the source may stall mid-download. Default `60` |
| `CLEAN_MAX_AGE_HOURS` | `24` | Optional. Age gate for weak orphan signals. Default `24` |
| `CLEAN_WEAK` | `1` | Optional. `0` = only delete proven orphans. Default `1` |
| `CLEAN_MAX_DELETES` | `200` | Optional. Cap per `/clean orphans` run. Default `200` |
| `LOG_MAX_BYTES` | `5242880` | Optional. Log rotation size. Default 5 MB |
| `LOG_BACKUPS` | `3` | Optional. Rotated log files kept. Default `3` |
| `JOB_LOG_MAX_BYTES` | `20971520` | Optional. Auto-compact `jobs.jsonl` above this. Default 20 MB |
| `JOB_LOG_KEEP` | `5000` | Optional. Records kept when compacting. Default `5000` |

Install and run (recommended, avoids the `externally-managed-environment` error):

```bash
uv venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python bot.py --log bot.log
```

`--log` writes to a size-rotated file (5 MB × 3 by default). Omit it to log to
stdout. **Run only one instance** — two bots sharing a token fight over
`getUpdates` and neither will answer commands.

Classic venv alternative:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python bot.py --log bot.log
```

Termux (Android):

```bash
pkg install python
pip install -r requirements.txt
python bot.py --log bot.log
```

## Usage

- Send any direct download link → bot streams it to `WEBDAV_URL/WEBDAV_DIR/`.
- Send **several links in one message** → each becomes its own job, uploaded in parallel.
- `/start`, `/help` — help text.
- `/ls` — list the target WebDAV folder.
- `/jobs` — show uploads in progress (id, file, progress bar, elapsed).
- `/cancel` — cancel your newest upload.
- `/cancel 12` — cancel job #12.
- `/cancel all` — cancel all your uploads.
- `/history` (optionally `/history 50`) — past jobs with per-outcome verdicts, read from disk.
- `/clean` — orphan/partial scan, deletes nothing. `/clean orphans` deletes, `/clean log` tidies logs, `/clean all` both.

> Job ids restart from 1 after every bot restart; `/history` is the reliable record across restarts.

> Jobs are numbered per bot run and only visible to the user who started them.

> Page links (video pages, download pages with timers/buttons) are **not** direct files. Open them in a browser, copy the final `.mp3`/`.mp4`/CDN URL, and send that. If `yt-dlp` is installed the bot attempts this resolution automatically.

## Files

- `bot.py` — the bot (parallel streaming upload, job registry, progress, `/jobs`, `/cancel`, `/history`, `/clean`, `/ls`).
- `requirements.txt` — `python-telegram-bot`, `requests`, `python-dotenv`, `yt-dlp`.
- `.env.example` — config template (copy to `.env`; `.env` is git-ignored and never committed).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `externally-managed-environment` on `pip install` | Use the `.venv` steps above, never `--break-system-packages`. |
| `403 ... Just a moment...` | Cloudflare challenge — send the final direct CDN URL instead. |
| `WebDAV PUT failed (400)` | Fixed: uploader now sends a clean `Content-Length` (no chunked+length combo). Update to latest `bot.py`. |
| `Missing TELEGRAM_BOT_TOKEN / WEBDAV_* in .env` | `.env` missing or empty — copy from `.env.example` and fill it. |
| `Conflict: terminated by other getUpdates request` | **Two instances of the bot are running.** Only one can hold the getUpdates lock, so commands go to whichever wins and the loser serves nothing. Check with `pgrep -af bot.py` and kill the extra. |
| `The write operation timed out` | TCP back-pressure: teracloud stopped draining the socket for longer than `STALL_TIMEOUT`. Two parallel multi-GB uploads saturating one uplink is the usual cause — lower `MAX_CONCURRENT_UPLOADS`. |

## Failure isolation

Jobs are fully independent. Each has its own `requests.Session`, its own cancel
flag and its own asyncio task, so one job failing never touches another — a
failure only releases its own semaphore slot. What looks like "one job killed
the rest" is usually several jobs independently hitting the same cause (e.g. all
of them write-stalling at once).

`/jobs` lists live uploads **and** the last 20 finished ones, so you can always
see what happened to the others.

### Job records

Every job transition — `started`, `done`, `failed`, `cancelled` — is appended to
`jobs.jsonl` (git-ignored) with timestamp, url, filename, dest url, bytes
transferred, duration and error text. Query it with `/history`, or directly:

```bash
jq -r 'select(.event=="failed") | [.ts,.job,.filename,.error] | @tsv' jobs.jsonl
```

A `started` record with no terminal record after it means the process died
mid-transfer — precisely the case `/clean` looks for.

### Orphan / partial cleanup

A failed `PUT` is ambiguous: the server may have discarded the truncated body,
stored it, or — if only the response was lost — stored the whole file. So the
bot never deletes blindly. `delete_partial_if_incomplete()` `HEAD`s the target
and compares the remote size against what the job was supposed to send:

| remote size | action |
|---|---|
| equals expected | **kept** — the server committed a good file |
| less than expected, or zero | deleted |
| `404` | nothing to do |

On **any** failure (timeout, reset, `4xx/5xx`) *and* on cancel, that sweep runs.

`/clean` then catches whatever predates this logic, cross-referencing
`jobs.jsonl` against a `PROPFIND` listing:

- **strong** — a non-terminal record for that exact name whose size disagrees
- **weak** (`CLEAN_WEAK=1`) — tiny `_N`-suffixed duplicate or long-dead zero-byte file, older than `CLEAN_MAX_AGE_HOURS`, with no successful record

Never applied to: a file with a matching `done` record, a directory, or anything
younger than `CLEAN_MIN_FREE_SECS` (could be in flight).
