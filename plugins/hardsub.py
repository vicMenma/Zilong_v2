"""
plugins/hardsub.py
CloudConvert-powered hardsubbing — burn subtitles into video via
CloudConvert's FFmpeg engine (much faster than Colab's free GPU).

Flow:
  /hardsub → video (file/URL/magnet) → subtitle (file/URL/.txt)
  → pick resolution → CloudConvert processes → webhook auto-uploads

Supports:
  Video:  Telegram file, direct URL, magnet, torrent, yt-dlp, gdrive
  Subs:   .ass .srt .vtt .ssa .sub .txt — as file or URL
  Scale:  Original, 1080p, 720p, 480p, 360p
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import urllib.parse as _urlparse

import aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from core.config import cfg
from core.session import users
from services.utils import human_size, make_tmp, cleanup, safe_edit

log = logging.getLogger(__name__)

_SUB_EXTS = {".ass", ".srt", ".vtt", ".ssa", ".sub", ".txt"}

# ── Per-user state machine ────────────────────────────────────
# Steps: waiting_video → waiting_subtitle → waiting_resolution → done
_STATE: dict[int, dict] = {}


def _user_state(uid: int) -> dict | None:
    return _STATE.get(uid)


def _clear(uid: int) -> None:
    s = _STATE.pop(uid, None)
    if s and s.get("tmp"):
        cleanup(s["tmp"])


# ── Resolution picker keyboard ────────────────────────────────

def _res_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Original",  callback_data=f"hs_res|0|{uid}"),
         InlineKeyboardButton("🔵 1080p",     callback_data=f"hs_res|1080|{uid}")],
        [InlineKeyboardButton("🟢 720p",      callback_data=f"hs_res|720|{uid}"),
         InlineKeyboardButton("🟡 480p",      callback_data=f"hs_res|480|{uid}")],
        [InlineKeyboardButton("🟠 360p",      callback_data=f"hs_res|360|{uid}"),
         InlineKeyboardButton("❌ Cancel",     callback_data=f"hs_res|cancel|{uid}")],
    ])


# ─────────────────────────────────────────────────────────────
# Shared: submit hardsub to CloudConvert
# ─────────────────────────────────────────────────────────────

async def _submit_to_cloudconvert(
    st, state: dict, sub_fname: str, uid: int, scale_height: int = 0,
) -> None:
    """Shared submission logic — called after resolution is picked."""
    video_fname = state.get("video_fname", "video.mkv")
    name_base = os.path.splitext(video_fname)[0]

    # Add resolution tag to output name if downscaling
    res_tag = f" [{scale_height}p VOSTFR]" if scale_height else " [VOSTFR]"
    output_name = re.sub(r'[^\w\s\-\[\]()]', '_', name_base).strip() + f"{res_tag}.mp4"

    res_label = f"{scale_height}p" if scale_height else "Original"
    await safe_edit(st,
        "☁️ <b>Submitting to CloudConvert…</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{video_fname[:42]}</code>\n"
        f"💬 <code>{sub_fname[:42]}</code>\n"
        f"📐 Resolution: <b>{res_label}</b>\n"
        f"📤 → <code>{output_name[:42]}</code>\n\n"
        "<i>CloudConvert will burn the subtitles and the webhook\n"
        "will auto-upload the result when ready.</i>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        api_key = os.environ.get("CC_API_KEY", "").strip()
        from services.cloudconvert_api import submit_hardsub, parse_api_keys, pick_best_key

        # Show key selection info
        keys = parse_api_keys(api_key)
        if len(keys) > 1:
            selected, credits = await pick_best_key(keys)
            key_info = f"🔑 Key {keys.index(selected)+1}/{len(keys)} ({credits} credits left)"
        else:
            key_info = "🔑 1 API key"

        job_id = await submit_hardsub(
            api_key,
            video_path=state.get("video_path"),
            video_url=state.get("video_url"),
            subtitle_path=state["sub_path"],
            output_name=output_name,
            scale_height=scale_height,
        )

        if state.get("video_url"):
            mode_s = "☁️ URL import (no upload needed)"
        else:
            vsize = os.path.getsize(state["video_path"]) if state.get("video_path") else 0
            mode_s = f"📤 Uploaded {human_size(vsize)}"

        await safe_edit(st,
            "✅ <b>Hardsub Job Submitted!</b>\n"
            "──────────────────────\n\n"
            f"🆔 <code>{job_id}</code>\n"
            f"🎬 <code>{video_fname[:38]}</code>\n"
            f"💬 <code>{sub_fname[:38]}</code>\n"
            f"📐 <b>{res_label}</b>\n"
            f"📦 → <code>{output_name[:38]}</code>\n"
            f"⚙️ {mode_s}\n"
            f"{key_info}\n\n"
            "⏳ <i>CloudConvert is processing…\n"
            "The webhook will auto-upload the result to this chat.</i>",
            parse_mode=enums.ParseMode.HTML,
        )

        log.info("[Hardsub] Job %s submitted for uid=%d: %s + %s → %s (%s)",
                 job_id, uid, video_fname, sub_fname, output_name, res_label)

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


# ── Show resolution picker ────────────────────────────────────

async def _show_resolution_picker(st, state: dict, sub_fname: str, uid: int) -> None:
    """Show inline buttons to pick output resolution."""
    video_fname = state.get("video_fname", "video.mkv")
    state["step"] = "waiting_resolution"
    state["_res_msg"] = st  # save reference for callback

    await safe_edit(st,
        "✅ <b>Subtitle ready!</b>\n"
        "──────────────────────\n\n"
        f"🎬 <code>{video_fname[:42]}</code>\n"
        f"💬 <code>{sub_fname[:42]}</code>\n\n"
        "📐 <b>Choose output resolution:</b>",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_res_kb(uid),
    )


# ─────────────────────────────────────────────────────────────
# /hardsub command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("hardsub"))
async def cmd_hardsub(client: Client, msg: Message):
    uid = msg.from_user.id
    await users.register(uid, msg.from_user.first_name or "")

    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        return await msg.reply(
            "❌ <b>CloudConvert API key not set</b>\n\n"
            "Add <code>CC_API_KEY=your_key</code> to your .env or Colab secrets.\n\n"
            "Get a key at: cloudconvert.com → Dashboard → API → API Keys",
            parse_mode=enums.ParseMode.HTML,
        )

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

@Client.on_message(
    filters.private & (filters.video | filters.document),
    group=1,
)
async def hardsub_video_file(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state or state["step"] != "waiting_video":
        return

    media = msg.video or msg.document
    if not media:
        return

    fname = getattr(media, "file_name", None) or "video.mkv"
    ext = os.path.splitext(fname)[1].lower()

    _VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts", ".wmv", ".m4v"}
    if ext not in _VIDEO_EXTS and not msg.video:
        return

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
            "Now send the <b>subtitle</b>:\n"
            "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
            "• A <b>URL</b> to a subtitle file",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                        parse_mode=enums.ParseMode.HTML)
        _clear(uid)

    msg.stop_propagation()


# Handle URL/magnet input (video step + subtitle step)
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
async def hardsub_url_handler(client: Client, msg: Message):
    uid = msg.from_user.id
    state = _user_state(uid)
    if not state:
        return
    if state["step"] not in ("waiting_video", "waiting_subtitle"):
        return

    text = msg.text.strip()

    url_re = re.compile(r"^(https?://\S+|magnet:\?\S+)$", re.I)
    if not url_re.match(text):
        return

    # ── Subtitle URL ──────────────────────────────────────────
    if state["step"] == "waiting_subtitle":
        await _handle_subtitle_url(msg, state, text, uid)
        msg.stop_propagation()
        return

    # ── Video URL ─────────────────────────────────────────────
    from services.downloader import classify
    kind = classify(text)

    if kind == "direct":
        state["video_url"] = text
        raw_name = text.split("/")[-1].split("?")[0]
        state["video_fname"] = _urlparse.unquote_plus(raw_name)[:50] or "video.mkv"
        state["step"] = "waiting_subtitle"

        await msg.reply(
            f"✅ Video URL received\n"
            f"<code>{text[:60]}</code>\n\n"
            "☁️ <i>CloudConvert will fetch this directly — no local download needed!</i>\n\n"
            "Now send the <b>subtitle</b>:\n"
            "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
            "• A <b>URL</b> to a subtitle file",
            parse_mode=enums.ParseMode.HTML,
        )
        msg.stop_propagation()

    elif kind in ("magnet", "torrent", "ytdlp", "gdrive", "mediafire"):
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
                "Now send the <b>subtitle</b>:\n"
                "• A <b>file</b> (.ass / .srt / .vtt / .txt)\n"
                "• A <b>URL</b> to a subtitle file",
                parse_mode=enums.ParseMode.HTML,
            )
        except Exception as exc:
            await safe_edit(st, f"❌ Download failed: <code>{exc}</code>",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)

        msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2a: Receive subtitle FILE
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

    if ext not in _SUB_EXTS:
        return

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

    # Show resolution picker
    await _show_resolution_picker(st, state, fname, uid)

    msg.stop_propagation()


# ─────────────────────────────────────────────────────────────
# Step 2b: Receive subtitle URL
# ─────────────────────────────────────────────────────────────

async def _handle_subtitle_url(msg: Message, state: dict, url: str, uid: int) -> None:
    """Download subtitle from URL, then show resolution picker."""
    tmp = state["tmp"]

    parsed_path = _urlparse.urlparse(url).path
    raw_fname = os.path.basename(parsed_path)
    fname = _urlparse.unquote_plus(raw_fname) if raw_fname else "subtitle.ass"

    ext = os.path.splitext(fname)[1].lower()
    if ext not in _SUB_EXTS:
        fname = fname + ".ass" if fname else "subtitle.ass"

    fname = re.sub(r'[\\/:*?"<>|]', "_", fname)

    st = await msg.reply(
        f"⬇️ Downloading subtitle from URL…\n"
        f"<code>{url[:60]}</code>",
        parse_mode=enums.ParseMode.HTML,
    )

    try:
        sub_path = os.path.join(tmp, fname)
        headers = {"User-Agent": "Mozilla/5.0"}

        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, headers=headers, allow_redirects=True) as resp:
                resp.raise_for_status()

                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    cd_fname = cd.split("filename=")[-1].strip().strip('"').strip("'")
                    if cd_fname:
                        cd_fname = _urlparse.unquote_plus(cd_fname)
                        cd_ext = os.path.splitext(cd_fname)[1].lower()
                        if cd_ext in _SUB_EXTS:
                            fname = re.sub(r'[\\/:*?"<>|]', "_", cd_fname)
                            sub_path = os.path.join(tmp, fname)

                content = await resp.read()

        if len(content) > 10_000_000:
            await safe_edit(st, "❌ File too large — doesn't look like a subtitle file.",
                            parse_mode=enums.ParseMode.HTML)
            _clear(uid)
            return

        with open(sub_path, "wb") as f:
            f.write(content)

        state["sub_path"] = sub_path
        state["sub_fname"] = fname

        log.info("[Hardsub] Subtitle downloaded from URL: %s (%s)",
                 fname, human_size(os.path.getsize(sub_path)))

    except Exception as exc:
        log.error("[Hardsub] Subtitle URL download failed: %s", exc)
        await safe_edit(st,
            f"❌ Subtitle download failed:\n<code>{str(exc)[:200]}</code>",
            parse_mode=enums.ParseMode.HTML,
        )
        _clear(uid)
        return

    # Show resolution picker
    await _show_resolution_picker(st, state, fname, uid)


# ─────────────────────────────────────────────────────────────
# Step 3: Resolution picker callback  hs_res|<height>|<uid>
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^hs_res\|"))
async def hardsub_resolution_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, height_str, uid_str = parts[:3]
    uid = int(uid_str) if uid_str.isdigit() else cb.from_user.id

    state = _user_state(uid)
    if not state or state["step"] != "waiting_resolution":
        return await cb.answer("Session expired. Use /hardsub again.", show_alert=True)

    await cb.answer()

    # Cancel
    if height_str == "cancel":
        _clear(uid)
        await cb.message.delete()
        return

    scale_height = int(height_str) if height_str.isdigit() else 0
    sub_fname = state.get("sub_fname", "subtitle.ass")

    # Submit to CloudConvert with chosen resolution
    await _submit_to_cloudconvert(cb.message, state, sub_fname, uid, scale_height)
