"""
plugins/ccstatus.py
/ccstatus — CloudConvert job status dashboard with live FFmpeg progress.

Features:
  - Shows all submitted hardsub/convert jobs with live status
  - Live FFmpeg encoding progress bar (polled every 5s while processing)
  - Background poller: 5s when any job is encoding, 60s when idle
  - Auto-notifies you the moment a job finishes or errors
  - Inline ♻️ Refresh and 🗑 Clear Finished buttons
  - Jobs persist across restarts via data/cc_jobs.json
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

from pyrogram import Client, filters, enums
from pyrogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from services.cc_job_store import CCJob, job_store
from services.utils import safe_edit

log = logging.getLogger(__name__)

# ── Poller intervals ──────────────────────────────────────────
_POLL_FAST = 5    # seconds — used when ≥1 job is actively encoding
_POLL_IDLE = 60   # seconds — used when all jobs are waiting/finished

_poller_started = False


# ─────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────

_STATUS_ICON = {
    "waiting":    "⏳",
    "processing": "🔄",
    "finished":   "✅",
    "error":      "❌",
}
_STATUS_LABEL = {
    "waiting":    "Queued",
    "processing": "Encoding",
    "finished":   "Finished",
    "error":      "Failed",
}


def _prog_bar(pct: float, cells: int = 12) -> str:
    filled = round(pct / 100 * cells)
    return "█" * filled + "░" * (cells - filled)


def _age(ts: float) -> str:
    s = int(time.time() - ts)
    if s < 60:   return f"{s}s ago"
    if s < 3600: return f"{s // 60}m ago"
    return f"{s // 3600}h {(s % 3600) // 60}m ago"


def _render_job(j: CCJob, idx: int) -> str:
    icon  = _STATUS_ICON.get(j.status, "❓")
    label = _STATUS_LABEL.get(j.status, j.status.upper())

    lines = [
        f"<b>[{idx}]</b>  {icon} <b>{label}</b>",
        f"  🎬 <code>{j.fname[:45]}</code>",
        f"  💬 <code>{j.sub_fname[:35]}</code>",
        f"  🆔 <code>{j.job_id}</code>",
    ]

    if j.status == "processing":
        pct = j.progress_pct
        bar = _prog_bar(pct)
        msg = j.task_message or "Executing ffmpeg"
        elapsed = int(time.time() - j.submitted_at)
        lines += [
            f"  📊 <code>[{bar}]</code>  <b>{pct:.1f}%</b>",
            f"  ⚙️ <i>{msg}</i>",
            f"  ⏱ {elapsed // 60}m {elapsed % 60}s elapsed",
        ]

    elif j.status == "waiting":
        elapsed = int(time.time() - j.submitted_at)
        lines.append(f"  ⏳ Submitted {_age(j.submitted_at)} — waiting to start")

    elif j.status == "finished" and j.finished_at:
        duration = int(j.finished_at - j.submitted_at)
        lines += [
            f"  ✅ Done in <b>{duration // 60}m {duration % 60}s</b>",
            f"  📁 <code>{j.output_name[:45]}</code>",
        ]

    elif j.status == "error":
        lines.append(f"  ⏱ {_age(j.submitted_at)}")
        if j.error_msg:
            lines.append(f"  ⚠️ <code>{j.error_msg[:80]}</code>")

    return "\n".join(lines)


def _render_panel(uid: int) -> str:
    jobs = job_store.jobs_for_user(uid)
    if not jobs:
        return (
            "☁️ <b>CloudConvert Status</b>\n"
            "──────────────────────\n\n"
            "<i>No jobs found.\n"
            "Submit one with /hardsub or the 🔥 Hardsub button on any URL.</i>"
        )

    active   = [j for j in jobs if j.status in ("waiting", "processing")]
    finished = [j for j in jobs if j.status == "finished"]
    errored  = [j for j in jobs if j.status == "error"]

    summary = []
    if active:   summary.append(f"🔄 {len(active)} processing")
    if finished: summary.append(f"✅ {len(finished)} finished")
    if errored:  summary.append(f"❌ {len(errored)} failed")

    lines = [
        "☁️ <b>CloudConvert Status</b>",
        f"<code>{'  ·  '.join(summary) if summary else 'idle'}</code>",
        "──────────────────────",
    ]

    for i, j in enumerate(jobs[:10], 1):
        lines.append("")
        lines.append(_render_job(j, i))

    if len(jobs) > 10:
        lines.append(f"\n<i>…and {len(jobs) - 10} more. Use 🗑 Clear Finished to clean up.</i>")

    # Show polling rate hint only when actively encoding
    encoding = [j for j in jobs if j.status == "processing"]
    if encoding:
        lines += [
            "",
            "──────────────────────",
            f"<i>🔁 Polling every {_POLL_FAST}s — auto-notified on completion</i>",
        ]
    else:
        lines += [
            "",
            "──────────────────────",
            "<i>Tap ♻️ Refresh to update · auto-notified on completion</i>",
        ]

    return "\n".join(lines)


def _status_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("♻️ Refresh",        callback_data=f"ccs|refresh|{uid}"),
         InlineKeyboardButton("🗑 Clear Finished", callback_data=f"ccs|clear|{uid}")],
        [InlineKeyboardButton("❌ Close",           callback_data=f"ccs|close|{uid}")],
    ])


# ─────────────────────────────────────────────────────────────
# /ccstatus command
# ─────────────────────────────────────────────────────────────

@Client.on_message(filters.private & filters.command("ccstatus"))
async def cmd_ccstatus(client: Client, msg: Message):
    uid = msg.from_user.id
    await _ensure_poller(client)
    text = _render_panel(uid)
    await msg.reply(text, parse_mode=enums.ParseMode.HTML,
                    reply_markup=_status_kb(uid))


# ─────────────────────────────────────────────────────────────
# Inline buttons
# ─────────────────────────────────────────────────────────────

@Client.on_callback_query(filters.regex(r"^ccs\|"))
async def ccs_cb(client: Client, cb: CallbackQuery):
    parts = cb.data.split("|")
    if len(parts) < 3:
        return await cb.answer("Invalid data.", show_alert=True)

    _, action, uid_str = parts[:3]
    uid = int(uid_str) if uid_str.isdigit() else cb.from_user.id
    await cb.answer()

    if action == "close":
        return await cb.message.delete()

    if action == "clear":
        removed = await job_store.clear_finished(uid)
        note = f"🗑 Cleared {removed} finished/failed job(s)." if removed else "Nothing to clear."
        await cb.answer(note, show_alert=True)

    if action in ("refresh", "clear"):
        await _ensure_poller(client)
        text = _render_panel(uid)
        try:
            await cb.message.edit(
                text,
                parse_mode=enums.ParseMode.HTML,
                reply_markup=_status_kb(uid),
            )
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                raise


# ─────────────────────────────────────────────────────────────
# Background poller
# ─────────────────────────────────────────────────────────────

async def _ensure_poller(client: Client) -> None:
    global _poller_started
    if not _poller_started:
        _poller_started = True
        asyncio.create_task(_poll_loop(client))
        log.info("[CCStatus] Background poller started")


async def _poll_loop(client: Client) -> None:
    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        log.warning("[CCStatus] No CC_API_KEY — poller will not run")
        return

    log.info("[CCStatus] Poller running")
    while True:
        try:
            await _sweep(client, api_key)
        except Exception as exc:
            log.warning("[CCStatus] Sweep error: %s", exc)

        # Use fast interval when any job is actively encoding
        active = job_store.all_active()
        encoding = any(j.status == "processing" for j in active)
        interval = _POLL_FAST if encoding else _POLL_IDLE
        await asyncio.sleep(interval)


async def _sweep(client: Client, api_key: str) -> None:
    """
    Check all active jobs once.

    For each job:
      - Extract percent + message from the active task → update progress bar
      - If status changed to finished/error → notify user
    """
    from services.cloudconvert_api import check_job_status

    active = job_store.all_active()
    if not active:
        return

    log.debug("[CCStatus] Sweeping %d active job(s)", len(active))

    for job in active:
        try:
            data   = await check_job_status(api_key, job.job_id)
            status = data.get("status", job.status)

            # ── Extract encoding progress from tasks ──────────
            updates: dict = {}
            processing_task = None

            for task in data.get("tasks", []):
                t_status = task.get("status", "")
                t_op     = task.get("operation", "")

                # Find the task that is currently encoding
                if t_status == "processing" and t_op == "command":
                    processing_task = task
                    pct = float(task.get("percent") or 0)
                    msg = task.get("message", "") or "Executing ffmpeg"
                    updates["progress_pct"]  = pct
                    updates["task_message"]  = msg
                    updates["progress_at"]   = time.time()
                    break

            # ── Status change handling ────────────────────────
            if status != job.status:
                updates["status"] = status

                if status in ("finished", "error"):
                    updates["finished_at"]   = time.time()
                    updates["progress_pct"]  = 100.0 if status == "finished" else job.progress_pct

                if status == "error":
                    for task in data.get("tasks", []):
                        if task.get("status") == "error":
                            updates["error_msg"] = task.get("message", "Unknown error")[:120]
                            break

            # ── Apply updates ─────────────────────────────────
            if updates:
                await job_store.update(job.job_id, **updates)
                log.info(
                    "[CCStatus] Job %s  status=%s  pct=%.1f%%  msg=%s",
                    job.job_id, updates.get("status", job.status),
                    updates.get("progress_pct", job.progress_pct),
                    updates.get("task_message", job.task_message),
                )

            # ── Notify user on terminal status ────────────────
            if status in ("finished", "error") and not job.notified:
                await _notify(client, job, status, data)
                await job_store.update(job.job_id, notified=True)

        except Exception as exc:
            log.warning("[CCStatus] Failed to check job %s: %s", job.job_id, exc)


async def _notify(client: Client, job: CCJob, status: str, data: dict) -> None:
    try:
        if status == "finished":
            output_name = job.output_name
            for task in data.get("tasks", []):
                if task.get("operation") == "export/url" and task.get("status") == "finished":
                    files = (task.get("result") or {}).get("files", [])
                    if files:
                        output_name = files[0].get("filename", output_name)
                    break

            duration = ""
            if job.finished_at:
                secs     = int(job.finished_at - job.submitted_at)
                duration = f"  ·  done in {secs // 60}m {secs % 60}s"

            text = (
                "☁️ <b>CloudConvert — Job Finished!</b>\n"
                "──────────────────────\n\n"
                f"🎬 <code>{job.fname[:45]}</code>\n"
                f"💬 <code>{job.sub_fname[:35]}</code>\n"
                f"📁 <code>{output_name[:45]}</code>\n"
                f"🆔 <code>{job.job_id}</code>{duration}\n\n"
                "⬆️ <i>The webhook is uploading the result to this chat…</i>\n\n"
                "<i>Use /ccstatus to check all jobs.</i>"
            )

        else:
            err = job.error_msg or "Unknown error"
            text = (
                "☁️ <b>CloudConvert — Job Failed</b>\n"
                "──────────────────────\n\n"
                f"🎬 <code>{job.fname[:45]}</code>\n"
                f"🆔 <code>{job.job_id}</code>\n\n"
                f"❌ <code>{err}</code>\n\n"
                "<i>Use /ccstatus to see all jobs.\n"
                "You can resubmit with /hardsub.</i>"
            )

        await client.send_message(
            job.uid, text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "📋 View All Jobs",
                    callback_data=f"ccs|refresh|{job.uid}",
                )]
            ]),
        )

    except Exception as exc:
        log.warning("[CCStatus] Could not notify uid=%d job %s: %s",
                    job.uid, job.job_id, exc)
