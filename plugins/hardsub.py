"""
plugins/hardsub.py
CloudConvert-powered hardsubbing — burn subtitles into video via
CloudConvert's FFmpeg engine (much faster than Colab's free GPU).

Usage:
  /hardsub  →  bot asks for video (file or URL) → then subtitle file
  CloudConvert does the heavy lifting → webhook auto-uploads result

Supports:
  - Telegram video file + subtitle file
  - URL/magnet + subtitle file (video downloaded first or imported by CC)
  - Direct HTTP URLs imported directly by CloudConvert (fastest — no upload)
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.utils import human_size, make_tmp, cleanup, safe_edit

log = logging.getLogger(__name__)

# ── Per-user state machine ────────────────────────────────────
# States: waiting_video → waiting_subtitle → processing
_STATE: dict[int, dict] = {}


def _user_state(uid: int) -> dict | None:
    return _STATE.get(uid)


def _clear(uid: int) -> None:
    s = _STATE.pop(uid, None)
    if s and s.get("tmp"):
        cleanup(s["tmp"])


# ─────────────────────────────────────────────────────────────
# /hardsub command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("hardsub"))
async def cmd_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid, msg.from_user.first_name or "")

    # Check if CC_API_KEY is configured
    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        return await msg.reply(
            "❌ <b>CloudConvert API key not set</b>\n\n"
            "Add <code>CC_API_KEY=your_key</code> to your .env or Colab secrets.\n\n"
            "Get a key at: cloudconvert.com → Dashboard → API → API Keys",
            parse_mode=enums.ParseMode.HTML,
        )

    # Clear any previous state
    _clear(uid)

    tmp = make_tmp(cfg.download_dir, uid)
    _STATE[uid] = {
        "step": "waiting_video",
        "tmp": tmp,
        "video_path": None,
        "video_url": None,
        "sub_path": None,
    }

    await msg.reply(
        "🔥 <b>CloudConvert Hardsub</b>\n"
        "──────────────────────\n\n"
        "Send me the <b>video</b>:\n"
        "• A <b>video file</b> (upload from Telegram)\n"
        "• A <b>direct URL</b> (HTTP link to .mkv/.mp4)\n"
        "• A <b>magnet link</b> (downloaded via aria2 first)\n\n"
        "<i>Send /cancel to abort.</i>",
        parse_mode=enums.ParseMode.HTML,
    )


@Client.on_message(filters.private & filters.command("cancel"), group=4)
async def cmd_cancel_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid in _STATE:
        _clear(uid)
        await msg.reply("❌ Hardsub cancelled.")
        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 1: Receive video (file or URL)
# ─────────────────────────────────────────────────────────────

# Handle video file
@Client.on_message(
    filters.private & (filters.video | filters.document),
    group=1,
)
async def hardsub_video_file(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_video":
        return  # Not in hardsub flow — let other handlers process

    media = msg.video or msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "video.mkv"
    ext = os.path.splitext(fname)[1].lower()

    # Only intercept video-like files
    _VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts", ".wmv", ".m4v"}
    if ext not in _VIDEO_EXTS and not msg.video:
        return  # Not a video — let media_router handle it

    fsize = getattr(media, "file_size", 0) or 0
    st = await msg.reply(
        f"⬇️ Downloading <code>{fname[:40]}</code>…",
        parse_mode=enums.ParseMode.HTML,
    )

    tmp = state["tmp"]
    try:
        from services.tg_download import tg_download
        path = await tg_download(
            client, media.file_id,
            os.path.join(tmp, fname), st,
            fname=fname, fsize=fsize, user_id=uid,
        )
        state["video_path"] = path
        state["video_fname"] = os.path.basename(path)
        state["step"] = "waiting_subtitle"

        await safe_edit(st,
            f"✅ Video received: <code>{fname[:40]}</code>\n"
            f"💾 <code>{human_size(fsize)}</code>\n\n"
            "Now send the <b>subtitle file</b> (.ass / .srt / .vtt)",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)

    msg.stop_propagation()


# Handle URL/magnet input
@Client.on_message(
    filters.private & filters.text & ~filters.command(
        ["start", "help", "settings", "info", "status", "log", "restart",
         "broadcast", "admin", "ban_user", "unban_user", "banned_list",
         "cancel", "show_thumb", "del_thumb", "json_formatter", "bulk_url",
         "hardsub", "stream", "forward", "createarchive", "archiveddone",
         "mergedone"]
    ),
    group=1,
)
async def hardsub_video_url(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_video":
        return

    text = msg.text.strip()

    # Check if it's a URL or magnet
    url_re = re.compile(r"^(https?://\S+|magnet:\?\S+)$", re.I)
    if not url_re.match(text):
        return  # Not a URL — let other handlers process

    from services.downloader import classify
    kind = classify(text)

    if kind == "direct":
        # Direct URL — CloudConvert can import it directly (fastest path)
        state["video_url"] = text
        state["video_fname"] = text.split("/")[-1].split("?")[0][:50] or "video.mkv"
        state["step"] = "waiting_subtitle"

        await msg.reply(
            f"✅ Video URL received\n"
            f"<code>{text[:60]}</code>\n\n"
            "☁️ <i>CloudConvert will fetch this directly — no local download needed!</i>\n\n"
            "Now send the <b>subtitle file</b> (.ass / .srt / .vtt)",
            parse_mode=enums.ParseMode.HTML,
        )
        msg.stop_propagation()

    elif kind in ("magnet", "torrent", "ytdlp", "gdrive", "mediafire"):
        # Need to download first, then upload to CC
        st = await msg.reply(
            f"⬇️ Downloading video via {kind}…\n"
            "<i>This may take a while for magnets.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        tmp = state["tmp"]
        try:
            from services.downloader import smart_download
            from services.utils import largest_file
            path = await smart_download(
                text, tmp,
                user_id=uid,
                label=f"Hardsub DL",
            )
            if os.path.isdir(path):
                resolved = largest_file(path)
                if resolved:
                    path = resolved

            if not os.path.isfile(path):
                raise FileNotFoundError("No output file found")

            state["video_path"] = path
            state["video_fname"] = os.path.basename(path)
            state["step"] = "waiting_subtitle"

            fsize = os.path.getsize(path)
            await safe_edit(st,
                f"✅ Video downloaded: <code>{os.path.basename(path)[:40]}</code>\n"
                f"💾 <code>{human_size(fsize)}</code>\n\n"
                "Now send the <b>subtitle file</b> (.ass / .srt / .vtt)",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as exc:
            await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)

        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2: Receive subtitle file
# ─────────────────────────────────────────────────────────────

@Client.on_message(
    filters.private & filters.document,
    group=0,
)
async def hardsub_subtitle_file(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_subtitle":
        return

    media = msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "subtitle.ass"
    ext = os.path.splitext(fname)[1].lower()

    _SUB_EXTS = {".ass", ".srt", ".vtt", ".ssa", ".sub"}
    if ext not in _SUB_EXTS:
        return  # Not a subtitle — let other handlers take it

    tmp = state["tmp"]
    st = await msg.reply("⬇️ Downloading subtitle…")

    try:
        sub_path = await client.download_media(
            media, file_name=os.path.join(tmp, fname)
        )
        state["sub_path"] = sub_path
        state["sub_fname"] = os.path.basename(sub_path)
    except Exception as exc:
        await safe_edit(st, f"❌ Subtitle download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)
        msg.stop_propagation()
        return

    msg.stop_propagation()

    # ── Ready to submit to CloudConvert ───────────────────────
    video_fname = state.get("video_fname", "video.mkv")
    name_base = os.path.splitext(video_fname)[0]
    # Clean up for output name
    output_name = re.sub(r'[^\w\s\-\[\]()]', '_', name_base).strip() + " [VOSTFR].mp4"

    await safe_edit(st,
        "☁️ <b>Submitting to CloudConvert…</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{video_fname[:45]}</code>\n"
        f"💬 <code>{fname[:45]}</code>\n"
        f"📤 → <code>{output_name[:45]}</code>\n\n"
        "<i>CloudConvert will burn the subtitles and the webhook\n"
        "will auto-upload the result when ready.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        api_key = os.environ.get("CC_API_KEY", "").strip()
        from services.cloudconvert_api import submit_hardsub

        job_id = await submit_hardsub(
            api_key,
            video_path=state.get("video_path"),
            video_url=state.get("video_url"),
            subtitle_path=state["sub_path"],
            output_name=output_name,
        )

        # Determine upload info
        if state.get("video_url"):
            mode_s = "☁️ URL import (no upload needed)"
        else:
            vsize = os.path.getsize(state["video_path"]) if state.get("video_path") else 0
            mode_s = f"📤 Uploaded {human_size(vsize)}"

        await safe_edit(st,
            "✅ <b>Hardsub Job Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"🎬 <code>{video_fname[:40]}</code>\n"
            f"💬 <code>{fname[:40]}</code>\n"
            f"📦 → <code>{output_name[:40]}</code>\n"
            f"⚙️ {mode_s}\n\n"
            "⏳ <i>CloudConvert is processing…\n"
            "The webhook will auto-upload the result to this chat.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        log.info("[Hardsub] Job %s submitted for uid=%d: %s + %s → %s",
                 job_id, uid, video_fname, fname, output_name)

    except Exception as exc:
        log.error("[Hardsub] Submit failed: %s", exc, exc_info=True)
        await safe_edit(st,
            f"❌ <b>CloudConvert submission failed</b>\n\n"
            f"<code>{str(exc)[:200]}</code>\n\n"
            "<i>Check your CC_API_KEY and try again.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    finally:
        _clear(uid)
