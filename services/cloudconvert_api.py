"""
services/cloudconvert_api.py
CloudConvert API v2 client — hardsubbing + conversion with multi-key rotation.

Multi-API key support:
  Set CC_API_KEY to a comma-separated list of keys:
    CC_API_KEY=eyJ...key1,eyJ...key2,eyJ...key3
  The bot checks remaining credits on each key via GET /v2/users/me
  and picks the one with the most minutes available.

⚠️  IMPORTANT: Always use a LIVE API key, not a Sandbox key.
    Sandbox keys cause instant ERROR on all jobs — they don't process files.
    Get a Live key at: cloudconvert.com → Dashboard → API → API Keys
    Make sure the toggle at the top of that page shows "Live", not "Sandbox".

Flows:
  1. submit_hardsub  — burn subtitles into video
  2. submit_convert  — resolution/format conversion (no subtitles)
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

CC_API = "https://api.cloudconvert.com/v2"

# How long to wait for an import/upload task to reach "waiting" state
# before giving up and raising an error.
_UPLOAD_READY_TIMEOUT = 30   # seconds
_UPLOAD_READY_POLL    = 1.5  # seconds between polls


# ─────────────────────────────────────────────────────────────
# Multi-key credit checking & rotation
# ─────────────────────────────────────────────────────────────

def parse_api_keys(raw: str) -> list[str]:
    """Parse comma-separated API keys from env var."""
    return [k.strip() for k in raw.split(",") if k.strip()]


def _safe_fname(name: str) -> str:
    """
    Strip ALL characters that could break FFmpeg argument string parsing
    on CloudConvert's remote shell:
      - spaces        → underscore
      - parentheses   → removed  (breaks shell argument splitting)
      - single quotes → removed  (breaks vf filter quoting)
      - brackets      → removed
      - colons        → removed  (breaks Windows paths / FFmpeg option syntax)
      - everything else non-word except dot and dash → underscore
    """
    import re as _re
    # Replace spaces with underscores first
    name = name.replace(" ", "_")
    # Remove characters that break shell/FFmpeg parsing
    name = _re.sub(r"[()\[\]'":;|&<>!@#$%^*+=`~]", "", name)
    # Collapse multiple underscores/dashes
    name = _re.sub(r"[_\-]{2,}", "_", name)
    return name.strip("_-") or "file"


async def check_credits(api_key: str) -> int:
    """
    Check remaining conversion credits for a single API key.
    Returns credits (minutes) remaining, or -1 on error.
    Also warns loudly if the key is a Sandbox key.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"{CC_API}/users/me", headers=headers) as resp:
                if resp.status != 200:
                    log.warning("[CC-API] Credit check failed: HTTP %d", resp.status)
                    return -1
                data = await resp.json()
                user = data.get("data", {})
                credits = user.get("credits", 0)

                # Detect sandbox keys — they have no credits and their username
                # often contains "sandbox", or the response includes a sandbox flag
                if user.get("sandbox"):
                    log.error(
                        "[CC-API] ⚠️  SANDBOX KEY DETECTED — "
                        "jobs will ERROR instantly. Use a Live API key."
                    )
                    return 0

                return int(credits)
    except Exception as exc:
        log.warning("[CC-API] Credit check error: %s", exc)
        return -1


async def pick_best_key(api_keys: list[str]) -> tuple[str, int]:
    """
    Check credits on all keys concurrently, return the one with most credits.
    Returns (best_key, credits). Raises RuntimeError if all keys exhausted.
    """
    if len(api_keys) == 1:
        credits = await check_credits(api_keys[0])
        if credits == 0:
            raise RuntimeError(
                "CloudConvert: 0 credits remaining on your only API key.\n"
                "Wait for daily reset or add more keys (comma-separated in CC_API_KEY).\n"
                "Also make sure you are using a LIVE key, not a Sandbox key."
            )
        return api_keys[0], max(credits, 0)

    tasks = [check_credits(key) for key in api_keys]
    results = await asyncio.gather(*tasks)

    best_key = ""
    best_credits = -1
    status_lines = []

    for key, credits in zip(api_keys, results):
        short = key[-8:]
        status_lines.append(f"  ...{short}: {credits} credits")
        if credits > best_credits:
            best_credits = credits
            best_key = key

    log.info("[CC-API] Key rotation — %d keys checked:\n%s",
             len(api_keys), "\n".join(status_lines))

    if best_credits <= 0:
        raise RuntimeError(
            f"CloudConvert: all {len(api_keys)} API keys exhausted (0 credits).\n"
            "Wait for daily reset or add more keys.\n"
            "Also make sure you are using LIVE keys, not Sandbox keys."
        )

    log.info("[CC-API] Selected key ...%s (%d credits remaining)", best_key[-8:], best_credits)
    return best_key, best_credits


# ─────────────────────────────────────────────────────────────
# Job creation — Hardsub
# ─────────────────────────────────────────────────────────────

async def create_hardsub_job(
    api_key: str,
    *,
    video_url: Optional[str] = None,
    video_filename: str = "video.mkv",
    subtitle_filename: str = "subtitle.ass",
    output_filename: str = "output.mp4",
    crf: int = 20,
    preset: str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = _safe_fname(video_filename)
    s_safe = _safe_fname(subtitle_filename)
    o_safe = _safe_fname(output_filename)

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url": video_url,
            "filename": v_safe,
        }
    else:
        tasks["import-video"] = {
            "operation": "import/upload",
        }

    tasks["import-sub"] = {
        "operation": "import/upload",
    }

    sub_path = f"/input/import-sub/{s_safe}"
    # CloudConvert passes -vf to FFmpeg via shell — single quotes inside
    # the filter string break argument splitting. _safe_fname already
    # removed all shell-unsafe characters so no quoting is needed.
    if scale_height > 0:
        vf = f"scale=-2:{scale_height},subtitles={sub_path}"
    else:
        vf = f"subtitles={sub_path}"
    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"-vf {vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {'128k' if scale_height and scale_height <= 480 else '192k'} "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    )

    tasks["hardsub"] = {
        "operation": "command",
        "input": ["import-video", "import-sub"],
        "engine": "ffmpeg",
        "command": "ffmpeg",
        "arguments": ffmpeg_args,
    }

    tasks["export"] = {
        "operation": "export/url",
        "input": ["hardsub"],
    }

    payload = {
        "tasks": tasks,
        "tag": "zilong-hardsub",
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert job creation failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Hardsub job created: id=%s  tasks=%d",
             job.get("id"), len(job.get("tasks", [])))
    return job


# ─────────────────────────────────────────────────────────────
# Job creation — Convert (resolution/format, no subtitles)
# ─────────────────────────────────────────────────────────────

async def create_convert_job(
    api_key: str,
    *,
    video_url: Optional[str] = None,
    video_filename: str = "video.mkv",
    output_filename: str = "output.mp4",
    crf: int = 20,
    preset: str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = _safe_fname(video_filename)
    o_safe = _safe_fname(output_filename)

    tasks: dict = {}

    if video_url:
        tasks["import-video"] = {
            "operation": "import/url",
            "url": video_url,
            "filename": v_safe,
        }
    else:
        tasks["import-video"] = {
            "operation": "import/upload",
        }

    if scale_height > 0:
        vf  = f"-vf scale=-2:{scale_height}"
        abr = "128k" if scale_height <= 480 else "192k"
    else:
        vf  = ""
        abr = "192k"

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"{vf} "
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {abr} "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    ).strip()

    tasks["convert"] = {
        "operation": "command",
        "input": ["import-video"],
        "engine": "ffmpeg",
        "command": "ffmpeg",
        "arguments": ffmpeg_args,
    }

    tasks["export"] = {
        "operation": "export/url",
        "input": ["convert"],
    }

    payload = {
        "tasks": tasks,
        "tag": "zilong-convert",
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{CC_API}/jobs", json=payload, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                error = data.get("message", str(data))
                raise RuntimeError(f"CloudConvert convert job failed ({resp.status}): {error}")

    job = data.get("data", data)
    log.info("[CC-API] Convert job created: id=%s  tasks=%d",
             job.get("id"), len(job.get("tasks", [])))
    return job


# ─────────────────────────────────────────────────────────────
# Upload helpers
# ─────────────────────────────────────────────────────────────

def _find_task(job: dict, name: str) -> Optional[dict]:
    for task in job.get("tasks", []):
        if task.get("name") == name:
            return task
    return None


async def _wait_for_upload_ready(
    api_key: str,
    job_id: str,
    task_name: str,
) -> dict:
    """
    Poll the CC API until the named import/upload task reaches 'waiting'
    status with a valid form URL. This is necessary because CC needs a
    moment after job creation to prepare S3 upload credentials.

    Without this wait, the bot tries to upload before the URL exists,
    causing an instant ERROR on the job.

    Returns the ready task dict.
    Raises RuntimeError if timeout is exceeded.
    """
    headers  = {"Authorization": f"Bearer {api_key}"}
    deadline = asyncio.get_event_loop().time() + _UPLOAD_READY_TIMEOUT

    log.info("[CC-API] Waiting for task '%s' to be ready for upload…", task_name)

    while asyncio.get_event_loop().time() < deadline:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
                if resp.status != 200:
                    await asyncio.sleep(_UPLOAD_READY_POLL)
                    continue
                data = await resp.json()

        job = data.get("data", data)

        # Check if entire job errored before we even start uploading
        if job.get("status") == "error":
            # Find the errored task for a useful message
            for task in job.get("tasks", []):
                if task.get("status") == "error":
                    msg = task.get("message", "Unknown error")
                    raise RuntimeError(
                        f"CloudConvert job errored before upload: {msg}\n"
                        "Make sure you are using a LIVE API key, not a Sandbox key."
                    )
            raise RuntimeError("CloudConvert job errored before upload could start.")

        task = _find_task(job, task_name)
        if not task:
            await asyncio.sleep(_UPLOAD_READY_POLL)
            continue

        status = task.get("status", "")
        result = task.get("result") or {}
        form   = result.get("form") or {}
        url    = form.get("url")

        if status == "waiting" and url:
            log.info("[CC-API] Task '%s' ready — upload URL obtained", task_name)
            return task

        if status == "error":
            msg = task.get("message", "Unknown error")
            raise RuntimeError(f"CloudConvert task '{task_name}' errored: {msg}")

        log.debug("[CC-API] Task '%s' status=%s — polling again…", task_name, status)
        await asyncio.sleep(_UPLOAD_READY_POLL)

    raise RuntimeError(
        f"CloudConvert task '{task_name}' did not become ready within "
        f"{_UPLOAD_READY_TIMEOUT}s. The API may be slow — try again."
    )


async def upload_file_to_task(
    task: dict,
    file_path: str,
    filename: Optional[str] = None,
) -> None:
    """
    Upload a local file to a CloudConvert import/upload task.
    The task must already be in 'waiting' status (use _wait_for_upload_ready first).
    """
    result = task.get("result") or {}
    form   = result.get("form") or {}
    url    = form.get("url")
    params = form.get("parameters") or {}

    if not url:
        raise RuntimeError(
            "No upload URL in task — task is not in 'waiting' state. "
            "Call _wait_for_upload_ready() before this function."
        )

    raw_fname = filename or os.path.basename(file_path)
    fname     = _safe_fname(raw_fname)  # ensure uploaded name matches what FFmpeg references
    fsize     = os.path.getsize(file_path)
    log.info("[CC-API] Uploading %s → %s (%d bytes) to %s…", raw_fname, fname, fsize, url[:60])

    data = aiohttp.FormData()
    for key, value in params.items():
        data.add_field(key, str(value))
    data.add_field(
        "file",
        open(file_path, "rb"),
        filename=fname,
    )

    async with aiohttp.ClientSession() as sess:
        async with sess.post(url, data=data, allow_redirects=True) as resp:
            if resp.status not in (200, 201, 204, 301, 302):
                body = await resp.text()
                raise RuntimeError(f"Upload failed ({resp.status}): {body[:300]}")

    log.info("[CC-API] Upload complete: %s", fname)


# ─────────────────────────────────────────────────────────────
# High-level submit — Hardsub (with auto key rotation)
# ─────────────────────────────────────────────────────────────

async def submit_hardsub(
    api_key: str,
    video_path: Optional[str] = None,
    video_url: Optional[str] = None,
    subtitle_path: str = "",
    output_name: str = "hardsub.mp4",
    crf: int = 20,
    scale_height: int = 0,
) -> str:
    if not video_path and not video_url:
        raise ValueError("Provide either video_path or video_url")
    if not subtitle_path or not os.path.isfile(subtitle_path):
        raise ValueError(f"Subtitle file not found: {subtitle_path}")

    keys = parse_api_keys(api_key)
    if not keys:
        raise ValueError("No API keys provided in CC_API_KEY")

    selected_key, credits = await pick_best_key(keys)
    log.info("[CC-API] Using key with %d credits remaining", credits)

    video_fname = (os.path.basename(video_path) if video_path
                   else video_url.split("/")[-1].split("?")[0])
    sub_fname   = os.path.basename(subtitle_path)

    job = await create_hardsub_job(
        selected_key,
        video_url=video_url if not video_path else None,
        video_filename=video_fname,
        subtitle_filename=sub_fname,
        output_filename=output_name,
        crf=crf,
        scale_height=scale_height,
    )

    job_id = job.get("id", "?")

    # ── Upload subtitle ───────────────────────────────────────
    # Always poll until ready — CC needs time to prepare S3 credentials
    sub_task = await _wait_for_upload_ready(selected_key, job_id, "import-sub")
    await upload_file_to_task(sub_task, subtitle_path, sub_fname)

    # ── Upload video (only when sending a file, not a URL) ────
    if video_path:
        vid_task = await _wait_for_upload_ready(selected_key, job_id, "import-video")
        await upload_file_to_task(vid_task, video_path, video_fname)

    log.info("[CC-API] Hardsub job fully submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# High-level submit — Convert (with auto key rotation)
# ─────────────────────────────────────────────────────────────

async def submit_convert(
    api_key: str,
    video_path: Optional[str] = None,
    video_url: Optional[str] = None,
    output_name: str = "converted.mp4",
    crf: int = 20,
    scale_height: int = 0,
) -> str:
    if not video_path and not video_url:
        raise ValueError("Provide either video_path or video_url")

    keys = parse_api_keys(api_key)
    if not keys:
        raise ValueError("No API keys provided in CC_API_KEY")

    selected_key, credits = await pick_best_key(keys)
    log.info("[CC-API] Convert: using key with %d credits remaining", credits)

    video_fname = (os.path.basename(video_path) if video_path
                   else video_url.split("/")[-1].split("?")[0])

    job = await create_convert_job(
        selected_key,
        video_url=video_url if not video_path else None,
        video_filename=video_fname,
        output_filename=output_name,
        crf=crf,
        scale_height=scale_height,
    )

    job_id = job.get("id", "?")

    # ── Upload video (only when sending a file, not a URL) ────
    if video_path:
        vid_task = await _wait_for_upload_ready(selected_key, job_id, "import-video")
        await upload_file_to_task(vid_task, video_path, video_fname)

    log.info("[CC-API] Convert job fully submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# Status check
# ─────────────────────────────────────────────────────────────

async def check_job_status(api_key: str, job_id: str) -> dict:
    """Check the status of a CloudConvert job."""
    keys = parse_api_keys(api_key)
    key  = keys[0] if keys else api_key
    headers = {"Authorization": f"Bearer {key}"}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json()
    return data.get("data", data)
