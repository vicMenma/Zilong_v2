"""
plugins/ccstatus.py
/ccstatus — CloudConvert job status dashboard.

Features:
  - Shows all your submitted hardsub/convert jobs with live status
  - Inline ♻️ Refresh and 🗑 Clear Finished buttons
  - Background poller runs every 60s, notifies you the moment a job
    finishes or errors (no need to manually check)
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

# ── Poller state ──────────────────────────────────────────────
_POLL_INTERVAL  = 60   # seconds between full sweeps
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
    "processing": "Processing",
    "finished":   "Finished",
    "error":      "Failed",
}


def _age(ts: float) -> str:
    """Human-readable time since ts."""
    s = int(time.time() - ts)
    if s < 60:   return f"{s}s ago"
    if s < 3600: return f"{s//60}m ago"
    return f"{s//3600}h {(s%3600)//60}m ago"


def _eta_guess(job: CCJob) -> str:
    """Rough ETA based on elapsed time — CC typically takes 2-5 min/episode."""
    elapsed = time.time() - job.submitted_at
    # Average encode time ~3 min; if we're past 10 min something is off
    remaining = max(0, 180 - elapsed)
    if elapsed > 600:
        return "⚠️ taking longer than usual"
    if remaining < 10:
        return "finishing soon…"
    return f"~{int(remaining//60)}m remaining (est.)"


def _render_job(j: CCJob, idx: int) -> str:
    icon  = _STATUS_ICON.get(j.status, "❓")
    label = _STATUS_LABEL.get(j.status, j.status.upper())
    lines = [
        f"<b>[{idx}]</b>  {icon} <b>{label}</b>",
        f"  🎬 <code>{j.fname[:45]}</code>",
        f"  💬 <code>{j.sub_fname[:35]}</code>",
        f"  🆔 <code>{j.job_id}</code>",
    ]
    if j.status in ("waiting", "processing"):
        lines.append(f"  ⏱ Submitted {_age(j.submitted_at)}  —  {_eta_guess(j)}")
    elif j.status == "finished" and j.finished_at:
        duration = int(j.finished_at - j.submitted_at)
        lines.append(f"  ✅ Done in {duration//60}m {duration%60}s")
        lines.append(f"  📁 <code>{j.output_name[:45]}</code>")
    elif j.status == "error":
        lines.append(f"  ⏱ Submitted {_age(j.submitted_at)}")
        if j.error_msg:
            lines.append(f"  💬 {j.error_msg[:80]}")
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
        f"<code>{'  ·  '.join(summary)}</code>",
        "──────────────────────",
    ]

    for i, j in enumerate(jobs[:10], 1):   # cap at 10 to avoid message too long
        lines.append("")
        lines.append(_render_job(j, i))

    if len(jobs) > 10:
        lines.append(f"\n<i>…and {len(jobs)-10} more. Use 🗑 Clear Finished to clean up.</i>")

    lines += ["", "──────────────────────",
              "<i>Tap ♻️ Refresh to update · auto-notified on completion</i>"]
    return "\n".join(lines)


def _status_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("♻️ Refresh",         callback_data=f"ccs|refresh|{uid}"),
         InlineKeyboardButton("🗑 Clear Finished",  callback_data=f"ccs|clear|{uid}")],
        [InlineKeyboardButton("❌ Close",            callback_data=f"ccs|close|{uid}")],
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
        # fall through to refresh

    if action in ("refresh", "clear"):
        await _ensure_poller(client)
        text = _render_panel(uid)
        try:
            await cb.message.edit(text, parse_mode=enums.ParseMode.HTML,
                                  reply_markup=_status_kb(uid))
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                raise


# ─────────────────────────────────────────────────────────────
# Background poller
# ─────────────────────────────────────────────────────────────

async def _ensure_poller(client: Client) -> None:
    """Start the background poller the first time it's needed."""
    global _poller_started
    if not _poller_started:
        _poller_started = True
        asyncio.create_task(_poll_loop(client))
        log.info("[CCStatus] Background poller started (interval=%ds)", _POLL_INTERVAL)


async def _poll_loop(client: Client) -> None:
    """
    Runs forever, checking all active CC jobs every _POLL_INTERVAL seconds.
    Sends a Telegram message to the user when a job finishes or errors.
    """
    api_key = os.environ.get("CC_API_KEY", "").strip()
    if not api_key:
        log.warning("[CCStatus] No CC_API_KEY — poller will not check job statuses")
        return

    log.info("[CCStatus] Poller running")
    while True:
        try:
            await _sweep(client, api_key)
        except Exception as exc:
            log.warning("[CCStatus] Sweep error: %s", exc)
        await asyncio.sleep(_POLL_INTERVAL)


async def _sweep(client: Client, api_key: str) -> None:
    """Check all active jobs once and notify users of any state changes."""
    from services.cloudconvert_api import check_job_status

    active = job_store.all_active()
    if not active:
        return

    log.debug("[CCStatus] Sweeping %d active job(s)", len(active))

    for job in active:
        try:
            data   = await check_job_status(api_key, job.job_id)
            status = data.get("status", "processing")

            if status == job.status:
                continue   # no change

            # ── Status changed ────────────────────────────────
            updates: dict = {"status": status}

            if status in ("finished", "error"):
                updates["finished_at"] = time.time()

            if status == "error":
                # Find the first errored task and grab its message
                for task in data.get("tasks", []):
                    if task.get("status") == "error":
                        updates["error_msg"] = task.get("message", "Unknown error")[:120]
                        break

            await job_store.update(job.job_id, **updates)
            log.info("[CCStatus] Job %s → %s (%s)", job.job_id, status, job.fname)

            # ── Notify user ───────────────────────────────────
            if not job.notified:
                await _notify(client, job, status, data)
                await job_store.update(job.job_id, notified=True)

        except Exception as exc:
            log.warning("[CCStatus] Failed to check job %s: %s", job.job_id, exc)


async def _notify(client: Client, job: CCJob, status: str, data: dict) -> None:
    """Send a Telegram notification to the user about a job state change."""
    try:
        if status == "finished":
            # Pull the output filename from the export task if available
            output_name = job.output_name
            for task in data.get("tasks", []):
                if task.get("operation") == "export/url" and task.get("status") == "finished":
                    files = (task.get("result") or {}).get("files", [])
                    if files:
                        output_name = files[0].get("filename", output_name)
                    break

            duration = ""
            if job.finished_at:
                secs = int(job.finished_at - job.submitted_at)
                duration = f"  ·  done in {secs//60}m {secs%60}s"

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

        else:  # error
            err = getattr(job, "error_msg", "") or "Unknown error"
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
                [InlineKeyboardButton("📋 View All Jobs", callback_data=f"ccs|refresh|{job.uid}")]
            ]),
        )

    except Exception as exc:
        log.warning("[CCStatus] Could not notify uid=%d for job %s: %s",
                    job.uid, job.job_id, exc)
