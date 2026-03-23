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
import time
from typing import Optional

import aiohttp

log = logging.getLogger(__name__)

CC_API = "https://api.cloudconvert.com/v2"

# How long to wait for an import/upload task to reach "waiting" state
_UPLOAD_READY_TIMEOUT = 30    # seconds
_UPLOAD_READY_POLL    = 1.5   # seconds between polls

# Minimum upload timeout regardless of file size
_UPLOAD_TIMEOUT_MIN = 120          # seconds
# Extra seconds per MB — assumes ~1.9 MB/s minimum Colab uplink
_UPLOAD_TIMEOUT_PER_MB = 8        # seconds / MB


# ─────────────────────────────────────────────────────────────
# Filename sanitiser
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
      - brackets, colons, and other shell specials → removed

    The sanitised name is used both in the FFmpeg command AND as the
    filename sent to S3 so they always match exactly.
    """
    import re as _re
    name = name.replace(" ", "_")
    name = _re.sub(r"[()[\]'\":;|&<>!@#$%^*+=`~]", "", name)
    name = _re.sub(r"[_\-]{2,}", "_", name)
    return name.strip("_-") or "file"


# ─────────────────────────────────────────────────────────────
# Multi-key credit checking & rotation
# ─────────────────────────────────────────────────────────────

async def check_credits(api_key: str) -> int:
    """
    Check remaining conversion credits for a single API key.
    Returns credits remaining, or -1 on error.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"{CC_API}/users/me", headers=headers) as resp:
                if resp.status != 200:
                    log.warning("[CC-API] Credit check failed: HTTP %d", resp.status)
                    return -1
                data = await resp.json()
                user    = data.get("data", {})
                credits = user.get("credits", 0)
                if user.get("sandbox"):
                    log.error(
                        "[CC-API] SANDBOX KEY DETECTED — "
                        "jobs will ERROR instantly. Use a Live API key."
                    )
                    return 0
                return int(credits)
    except Exception as exc:
        log.warning("[CC-API] Credit check error: %s", exc)
        return -1


async def pick_best_key(api_keys: list[str]) -> tuple[str, int]:
    """Return the key with the most credits. Raises if all exhausted."""
    if len(api_keys) == 1:
        credits = await check_credits(api_keys[0])
        if credits == 0:
            raise RuntimeError(
                "CloudConvert: 0 credits remaining.\n"
                "Wait for daily reset or add more keys (comma-separated in CC_API_KEY).\n"
                "Also make sure you are using a LIVE key, not a Sandbox key."
            )
        return api_keys[0], max(credits, 0)

    results  = await asyncio.gather(*[check_credits(k) for k in api_keys])
    best_key = ""
    best_crd = -1
    lines    = []

    for key, crd in zip(api_keys, results):
        lines.append(f"  ...{key[-8:]}: {crd} credits")
        if crd > best_crd:
            best_crd = crd
            best_key = key

    log.info("[CC-API] Key rotation — %d keys:\n%s", len(api_keys), "\n".join(lines))

    if best_crd <= 0:
        raise RuntimeError(
            f"CloudConvert: all {len(api_keys)} API keys exhausted (0 credits).\n"
            "Wait for daily reset or add more keys.\n"
            "Make sure you are using LIVE keys, not Sandbox keys."
        )

    log.info("[CC-API] Selected key ...%s (%d credits)", best_key[-8:], best_crd)
    return best_key, best_crd


# ─────────────────────────────────────────────────────────────
# Job creation — Hardsub
# ─────────────────────────────────────────────────────────────

async def create_hardsub_job(
    api_key: str,
    *,
    video_url:         Optional[str] = None,
    video_filename:    str = "video.mkv",
    subtitle_filename: str = "subtitle.ass",
    output_filename:   str = "output.mp4",
    crf:          int = 20,
    preset:       str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = _safe_fname(video_filename)
    s_safe = _safe_fname(subtitle_filename)
    o_safe = _safe_fname(output_filename)

    job_tasks: dict = {}

    if video_url:
        job_tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        job_tasks["import-video"] = {"operation": "import/upload"}

    job_tasks["import-sub"] = {"operation": "import/upload"}

    sub_path = f"/input/import-sub/{s_safe}"
    # No quotes around the subtitle path — _safe_fname removed all shell-unsafe
    # characters so the bare path is safe and avoids shell quoting conflicts.
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

    job_tasks["hardsub"] = {
        "operation": "command",
        "input":     ["import-video", "import-sub"],
        "engine":    "ffmpeg",
        "command":   "ffmpeg",
        "arguments": ffmpeg_args,
    }
    job_tasks["export"] = {"operation": "export/url", "input": ["hardsub"]}

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{CC_API}/jobs",
            json={"tasks": job_tasks, "tag": "zilong-hardsub"},
            headers=headers,
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"CloudConvert job creation failed ({resp.status}): "
                    f"{data.get('message', str(data))}"
                )

    job = data.get("data", data)
    log.info("[CC-API] Hardsub job created: id=%s", job.get("id"))
    return job


# ─────────────────────────────────────────────────────────────
# Job creation — Convert (no subtitles)
# ─────────────────────────────────────────────────────────────

async def create_convert_job(
    api_key: str,
    *,
    video_url:       Optional[str] = None,
    video_filename:  str = "video.mkv",
    output_filename: str = "output.mp4",
    crf:          int = 20,
    preset:       str = "medium",
    scale_height: int = 0,
) -> dict:
    v_safe = _safe_fname(video_filename)
    o_safe = _safe_fname(output_filename)

    job_tasks: dict = {}

    if video_url:
        job_tasks["import-video"] = {
            "operation": "import/url",
            "url":       video_url,
            "filename":  v_safe,
        }
    else:
        job_tasks["import-video"] = {"operation": "import/upload"}

    vf_flag = f"-vf scale=-2:{scale_height} " if scale_height > 0 else ""
    abr     = "128k" if scale_height > 0 and scale_height <= 480 else "192k"

    ffmpeg_args = (
        f"-i /input/import-video/{v_safe} "
        f"{vf_flag}"
        f"-c:v libx264 -crf {crf} -preset {preset} "
        f"-c:a aac -b:a {abr} "
        f"-movflags +faststart "
        f"/output/{o_safe}"
    ).strip()

    job_tasks["convert"] = {
        "operation": "command",
        "input":     ["import-video"],
        "engine":    "ffmpeg",
        "command":   "ffmpeg",
        "arguments": ffmpeg_args,
    }
    job_tasks["export"] = {"operation": "export/url", "input": ["convert"]}

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{CC_API}/jobs",
            json={"tasks": job_tasks, "tag": "zilong-convert"},
            headers=headers,
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(
                    f"CloudConvert convert job failed ({resp.status}): "
                    f"{data.get('message', str(data))}"
                )

    job = data.get("data", data)
    log.info("[CC-API] Convert job created: id=%s", job.get("id"))
    return job


# ─────────────────────────────────────────────────────────────
# Upload task readiness poller
# ─────────────────────────────────────────────────────────────

def _find_task(job: dict, name: str) -> Optional[dict]:
    for task in job.get("tasks", []):
        if task.get("name") == name:
            return task
    return None


async def _wait_for_upload_ready(api_key: str, job_id: str, task_name: str) -> dict:
    """
    Poll until the named import/upload task is in 'waiting' state with
    a valid S3 form URL. CC needs ~1-4s after job creation to prepare
    upload credentials — uploading before this causes instant ERROR.
    """
    headers  = {"Authorization": f"Bearer {api_key}"}
    deadline = asyncio.get_event_loop().time() + _UPLOAD_READY_TIMEOUT

    log.info("[CC-API] Waiting for task '%s' upload URL…", task_name)

    while asyncio.get_event_loop().time() < deadline:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
                if resp.status != 200:
                    await asyncio.sleep(_UPLOAD_READY_POLL)
                    continue
                data = await resp.json()

        job = data.get("data", data)

        if job.get("status") == "error":
            for t in job.get("tasks", []):
                if t.get("status") == "error":
                    raise RuntimeError(
                        f"CC job errored before upload: {t.get('message', 'Unknown')}\n"
                        "Make sure you are using a LIVE API key, not a Sandbox key."
                    )
            raise RuntimeError("CC job errored before upload could start.")

        task   = _find_task(job, task_name)
        status = (task or {}).get("status", "")
        url    = (((task or {}).get("result") or {}).get("form") or {}).get("url")

        if task and status == "waiting" and url:
            log.info("[CC-API] Task '%s' ready for upload", task_name)
            return task
        if task and status == "error":
            raise RuntimeError(
                f"CC task '{task_name}' errored: {task.get('message', 'Unknown')}"
            )

        await asyncio.sleep(_UPLOAD_READY_POLL)

    raise RuntimeError(
        f"CC task '{task_name}' not ready within {_UPLOAD_READY_TIMEOUT}s — try again."
    )


# ─────────────────────────────────────────────────────────────
# Streaming file uploader with live progress
# ─────────────────────────────────────────────────────────────

class _TrackingReader:
    """
    Thin wrapper around a binary file that tracks bytes read.
    aiohttp calls .read(n) on form field payloads, so wrapping the file
    object here gives a synchronous progress hook at zero overhead.
    """
    def __init__(self, path: str) -> None:
        self._f         = open(path, "rb")
        self.total      = os.path.getsize(path)
        self.bytes_read = 0

    def read(self, n: int = -1) -> bytes:
        chunk = self._f.read(n)
        self.bytes_read += len(chunk)
        return chunk

    def __len__(self) -> int:
        return self.total

    def close(self) -> None:
        self._f.close()


async def upload_file_to_task(
    task:      dict,
    file_path: str,
    filename:  Optional[str] = None,
    user_id:   int = 0,
    label:     str = "",
) -> None:
    """
    Stream a local file to a CloudConvert import/upload task with:
      - Streaming body    — never loads the whole file into memory
      - Live progress     — updates the bot's unified progress panel every second
      - Dynamic timeout   — scales with file size, never times out on large files
      - Auto-retry        — up to 3 attempts on transient network errors

    The task must already be in 'waiting' status (call _wait_for_upload_ready first).
    """
    result = task.get("result") or {}
    form   = result.get("form") or {}
    url    = form.get("url")
    params = form.get("parameters") or {}

    if not url:
        raise RuntimeError(
            "No upload URL in task — not in 'waiting' state. "
            "Call _wait_for_upload_ready() first."
        )

    raw_fname = filename or os.path.basename(file_path)
    fname     = _safe_fname(raw_fname)
    fsize     = os.path.getsize(file_path)
    fsize_mb  = fsize / (1024 * 1024)

    # Timeout scales with file size: min 2 min, +8s/MB, cap 2h
    timeout_secs = min(
        max(_UPLOAD_TIMEOUT_MIN, int(fsize_mb * _UPLOAD_TIMEOUT_PER_MB)),
        7200,
    )

    log.info(
        "[CC-API] Uploading %s → %s  (%.1f MB, timeout=%ds)",
        raw_fname, fname, fsize_mb, timeout_secs,
    )

    # ── Register with task tracker so progress panel shows the upload ────
    record = None
    if user_id:
        try:
            from services.task_runner import tracker, TaskRecord
            tid    = tracker.new_tid()
            record = TaskRecord(
                tid=tid, user_id=user_id,
                label=label or "Upload to CloudConvert",
                mode="ul", engine="direct",
                fname=fname, total=fsize,
                state="📤 Uploading to CloudConvert…",
            )
            await tracker.register(record)
        except Exception as _te:
            log.debug("[CC-API] Tracker unavailable: %s", _te)

    start = time.time()

    async def _poll_progress(reader: _TrackingReader) -> None:
        """Background task — samples reader.bytes_read every 1s and pushes to panel."""
        if record is None:
            return
        try:
            from services.task_runner import runner
            while True:
                await asyncio.sleep(1.0)
                done    = reader.bytes_read
                elapsed = time.time() - start
                speed   = done / elapsed if elapsed else 0.0
                eta     = int((fsize - done) / speed) if speed and done < fsize else 0
                record.update(
                    done=done, total=fsize,
                    speed=speed, eta=eta, elapsed=elapsed,
                    state="📤 Uploading to CloudConvert…",
                )
                runner._wake_panel(user_id)
                if done >= fsize:
                    break
        except asyncio.CancelledError:
            pass

    # ── Upload with retry ─────────────────────────────────────────────────
    MAX_ATTEMPTS = 3
    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        reader        = _TrackingReader(file_path)
        progress_task: Optional[asyncio.Task] = None

        try:
            progress_task = asyncio.create_task(_poll_progress(reader))

            # S3 presigned POST: policy fields must come BEFORE the file field
            form_data = aiohttp.FormData()
            for key, value in params.items():
                form_data.add_field(key, str(value))
            form_data.add_field(
                "file",
                reader,                           # aiohttp reads via .read() calls
                filename=fname,
                content_type="application/octet-stream",
            )

            timeout = aiohttp.ClientTimeout(total=timeout_secs)
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                    url, data=form_data,
                    allow_redirects=True,
                    timeout=timeout,
                ) as resp:
                    if resp.status not in (200, 201, 204, 301, 302):
                        body = await resp.text()
                        raise RuntimeError(
                            f"S3 upload rejected ({resp.status}): {body[:300]}"
                        )

            # ── Success ───────────────────────────────────────
            elapsed = time.time() - start
            speed   = fsize_mb / elapsed if elapsed else 0
            log.info(
                "[CC-API] Upload complete: %s  %.1f MB in %.1fs  (%.1f MB/s)",
                fname, fsize_mb, elapsed, speed,
            )
            if record is not None:
                record.update(done=fsize, total=fsize, state="✅ Done", elapsed=elapsed)
                try:
                    from services.task_runner import runner
                    runner._wake_panel(user_id, immediate=True)
                except Exception:
                    pass
            return  # success — exit retry loop

        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            last_exc = exc
            log.warning(
                "[CC-API] Upload attempt %d/%d failed for %s: %s",
                attempt, MAX_ATTEMPTS, fname, exc,
            )
            if attempt < MAX_ATTEMPTS:
                wait = 5 * attempt
                log.info("[CC-API] Retrying in %ds…", wait)
                await asyncio.sleep(wait)

        finally:
            reader.close()
            if progress_task and not progress_task.done():
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass

    # All attempts failed
    if record is not None:
        record.update(state=f"❌ Upload failed after {MAX_ATTEMPTS} attempts")
    raise RuntimeError(
        f"CloudConvert upload failed after {MAX_ATTEMPTS} attempts: {last_exc}"
    )


# ─────────────────────────────────────────────────────────────
# High-level submit — Hardsub
# ─────────────────────────────────────────────────────────────

async def submit_hardsub(
    api_key:       str,
    video_path:    Optional[str] = None,
    video_url:     Optional[str] = None,
    subtitle_path: str = "",
    output_name:   str = "hardsub.mp4",
    crf:           int = 20,
    scale_height:  int = 0,
    user_id:       int = 0,
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

    # ── Upload subtitle (small, always fast) ──────────────────
    sub_task = await _wait_for_upload_ready(selected_key, job_id, "import-sub")
    await upload_file_to_task(
        sub_task, subtitle_path, sub_fname,
        user_id=user_id,
        label=f"Sub → CC: {_safe_fname(sub_fname)}",
    )

    # ── Upload video file (only when not using a URL) ─────────
    if video_path:
        vid_task = await _wait_for_upload_ready(selected_key, job_id, "import-video")
        await upload_file_to_task(
            vid_task, video_path, video_fname,
            user_id=user_id,
            label=f"Video → CC: {_safe_fname(video_fname)}",
        )

    log.info("[CC-API] Hardsub job fully submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# High-level submit — Convert
# ─────────────────────────────────────────────────────────────

async def submit_convert(
    api_key:      str,
    video_path:   Optional[str] = None,
    video_url:    Optional[str] = None,
    output_name:  str = "converted.mp4",
    crf:          int = 20,
    scale_height: int = 0,
    user_id:      int = 0,
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

    if video_path:
        vid_task = await _wait_for_upload_ready(selected_key, job_id, "import-video")
        await upload_file_to_task(
            vid_task, video_path, video_fname,
            user_id=user_id,
            label=f"Video → CC: {_safe_fname(video_fname)}",
        )

    log.info("[CC-API] Convert job fully submitted: %s → %s", job_id, output_name)
    return job_id


# ─────────────────────────────────────────────────────────────
# Status check
# ─────────────────────────────────────────────────────────────

async def check_job_status(api_key: str, job_id: str) -> dict:
    """Check the status of a CloudConvert job."""
    keys    = parse_api_keys(api_key)
    key     = keys[0] if keys else api_key
    headers = {"Authorization": f"Bearer {key}"}
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{CC_API}/jobs/{job_id}", headers=headers) as resp:
            data = await resp.json()
    return data.get("data", data)
