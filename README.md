# infi-TG Bot

Telegram bot that accepts a **download link** and uploads it **directly to InfiniCloud WebDAV** — streamed chunk-by-chunk, no local disk usage.

## How it works

1. You send an `http(s)` link to the bot.
2. The bot `GET`s the file with browser-like headers and pipes it into a WebDAV `PUT` to your InfiniCloud (`https://XXXX.infini-cloud.net/dav/` or `teracloud.jp`).
3. No temp files — 256 KB chunks flow download → upload with a live progress bar in chat.

Extras:

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

Install and run (recommended, avoids the `externally-managed-environment` error):

```bash
uv venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python bot.py
```

Classic venv alternative:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python bot.py
```

Termux (Android):

```bash
pkg install python
pip install -r requirements.txt
python bot.py
```

## Usage

- Send any direct download link → bot streams it to `WEBDAV_URL/WEBDAV_DIR/`.
- `/start`, `/help` — help text.
- `/ls` — list the target WebDAV folder.

> Page links (video pages, download pages with timers/buttons) are **not** direct files. Open them in a browser, copy the final `.mp3`/`.mp4`/CDN URL, and send that. If `yt-dlp` is installed the bot attempts this resolution automatically.

## Files

- `bot.py` — the bot (streaming upload, progress, `/ls`).
- `requirements.txt` — `python-telegram-bot`, `requests`, `python-dotenv`, `yt-dlp`.
- `.env.example` — config template (copy to `.env`; `.env` is git-ignored and never committed).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `externally-managed-environment` on `pip install` | Use the `.venv` steps above, never `--break-system-packages`. |
| `403 ... Just a moment...` | Cloudflare challenge — send the final direct CDN URL instead. |
| `WebDAV PUT failed (400)` | Fixed: uploader now sends a clean `Content-Length` (no chunked+length combo). Update to latest `bot.py`. |
| `Missing TELEGRAM_BOT_TOKEN / WEBDAV_* in .env` | `.env` missing or empty — copy from `.env.example` and fill it. |
