"""
services/telegraph.py
Post MediaInfo to Telegra.ph.
Token is persisted to /tmp/zilong_telegraph.token so Colab restarts
don't create a new orphaned account every time.
"""
from __future__ import annotations

import aiohttp
import re

_TOKEN_FILE = "/tmp/zilong_telegraph.token"
_BASE       = "https://api.telegra.ph"
_token: str = ""


async def _get_token() -> str:
    global _token
    if _token:
        return _token
    try:
        with open(_TOKEN_FILE) as f:
            _token = f.read().strip()
        if _token:
            return _token
    except FileNotFoundError:
        pass
    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{_BASE}/createAccount", json={
            "short_name":  "ZilongBot",
            "author_name": "Zilong MediaInfo",
        }) as r:
            data = await r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegraph createAccount failed: {data}")
    _token = data["result"]["access_token"]
    try:
        with open(_TOKEN_FILE, "w") as f:
            f.write(_token)
    except Exception:
        pass
    return _token


async def post_mediainfo(filename: str, text: str) -> str:
    token = await _get_token()
    title = f"MediaInfo — {filename[:55]}"
    clean = re.sub(r'(Complete name\s*:\s*)/[^\n]*/', r'\1', text)
    clean = re.sub(r'/(?:tmp|content|home)/[^\s]*/([^\s/\n]+)', r'\1', clean)
    if len(clean) > 60_000:
        clean = clean[:60_000] + "\n\n...(truncated)"

    # ── Mobile-friendly rendering ─────────────────────────────────────────────
    # A single <pre> block works on desktop (horizontal scroll) but on mobile
    # Telegraph has no horizontal scroll — long lines overflow off-screen.
    # Solution: one <p> node per line. Section headers (all-caps like "General",
    # "Video", "Audio") get a <strong> tag so they stand out visually.
    # Empty lines become a <br> spacer for breathing room between sections.
    nodes = [
        {"tag": "p", "children": [{"tag": "em", "children": [filename]}]},
    ]

    _SECTION_RE = re.compile(r'^[A-Z][a-zA-Z\s#0-9]+$')

    for line in clean.splitlines():
        stripped = line.rstrip()

        if not stripped:
            nodes.append({"tag": "p", "children": [{"tag": "br", "children": []}]})
            continue

        # Section header — bold
        if _SECTION_RE.match(stripped) and len(stripped) < 40 and ":" not in stripped:
            nodes.append({"tag": "p", "children": [
                {"tag": "strong", "children": [stripped]}
            ]})
            continue

        # Key : Value line — split so key is plain and value is code
        if ":" in stripped:
            key, _, val = stripped.partition(":")
            key_s = key.rstrip()
            val_s = val.strip()
            children: list = [key_s + " : "]
            if val_s:
                children.append({"tag": "code", "children": [val_s]})
            nodes.append({"tag": "p", "children": children})
        else:
            nodes.append({"tag": "p", "children": [
                {"tag": "code", "children": [stripped]}
            ]})

    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{_BASE}/createPage", json={
            "access_token":  token,
            "title":         title,
            "author_name":   "Zilong Bot",
            "content":       nodes,
            "return_content": False,
        }) as r:
            data = await r.json()

    if data.get("ok"):
        return "https://telegra.ph/" + data["result"]["path"]
    raise RuntimeError(f"Telegraph createPage failed: {data.get('error','unknown')}")
