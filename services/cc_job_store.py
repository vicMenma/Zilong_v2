"""
services/cc_job_store.py
Persistent registry of submitted CloudConvert jobs.

Stores to data/cc_jobs.json so jobs survive bot restarts.
Each entry tracks: job_id, uid, filenames, status, timing, notification flag.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

_STORE_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "cc_jobs.json")
)
os.makedirs(os.path.dirname(_STORE_FILE), exist_ok=True)

# How long to keep finished/errored jobs visible before auto-eviction
JOB_LINGER_SECS = 3600 * 6  # 6 hours


@dataclass
class CCJob:
    job_id:       str
    uid:          int
    fname:        str           # video filename shown to user
    sub_fname:    str           # subtitle filename
    output_name:  str           # expected output filename
    submitted_at: float = field(default_factory=time.time)
    status:       str   = "processing"  # waiting|processing|finished|error
    error_msg:    str   = ""
    finished_at:  Optional[float] = None
    notified:     bool  = False  # True once user was sent a completion message


class CCJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, CCJob] = {}
        self._lock = asyncio.Lock()
        self._load()

    # ── Persistence ───────────────────────────────────────────

    def _load(self) -> None:
        try:
            with open(_STORE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            for entry in data:
                # Guard against missing keys from older schema
                entry.setdefault("notified", False)
                entry.setdefault("error_msg", "")
                entry.setdefault("finished_at", None)
                j = CCJob(**entry)
                self._jobs[j.job_id] = j
            log.info("[CCStore] Loaded %d job(s) from disk", len(self._jobs))
        except FileNotFoundError:
            pass
        except Exception as exc:
            log.warning("[CCStore] Load error: %s", exc)

    def _save(self) -> None:
        try:
            os.makedirs(os.path.dirname(_STORE_FILE), exist_ok=True)
            entries = [asdict(j) for j in self._jobs.values()]
            with open(_STORE_FILE, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2)
        except Exception as exc:
            log.warning("[CCStore] Save error: %s", exc)

    # ── Eviction ──────────────────────────────────────────────

    def _evict(self) -> None:
        """Remove old terminal jobs silently."""
        now  = time.time()
        dead = [
            jid for jid, j in self._jobs.items()
            if j.status in ("finished", "error")
            and j.finished_at is not None
            and now - j.finished_at > JOB_LINGER_SECS
        ]
        for jid in dead:
            self._jobs.pop(jid, None)

    # ── Mutations ─────────────────────────────────────────────

    async def add(self, job: CCJob) -> None:
        async with self._lock:
            self._evict()
            self._jobs[job.job_id] = job
            self._save()
        log.info("[CCStore] Registered job %s (%s)", job.job_id, job.fname)

    async def update(self, job_id: str, **kw) -> None:
        async with self._lock:
            j = self._jobs.get(job_id)
            if j:
                for k, v in kw.items():
                    if hasattr(j, k):
                        setattr(j, k, v)
                self._save()

    async def remove(self, job_id: str) -> None:
        async with self._lock:
            self._jobs.pop(job_id, None)
            self._save()

    async def clear_finished(self, uid: int) -> int:
        """Remove all finished/errored jobs for a user. Returns count removed."""
        async with self._lock:
            before = len(self._jobs)
            self._jobs = {
                jid: j for jid, j in self._jobs.items()
                if not (j.uid == uid and j.status in ("finished", "error"))
            }
            removed = before - len(self._jobs)
            if removed:
                self._save()
            return removed

    # ── Queries ───────────────────────────────────────────────

    def get(self, job_id: str) -> Optional[CCJob]:
        return self._jobs.get(job_id)

    def jobs_for_user(self, uid: int) -> list[CCJob]:
        self._evict()
        return sorted(
            [j for j in self._jobs.values() if j.uid == uid],
            key=lambda j: j.submitted_at,
            reverse=True,
        )

    def all_active(self) -> list[CCJob]:
        """Jobs still in waiting or processing state — used by the poller."""
        return [
            j for j in self._jobs.values()
            if j.status in ("waiting", "processing")
        ]

    def all_jobs(self) -> list[CCJob]:
        return list(self._jobs.values())


# Singleton shared across all plugins
job_store = CCJobStore()
