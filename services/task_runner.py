"""
services/task_runner.py
Global task registry + unified live progress panel.

Changes:
- MAX_CONCURRENT = 5: hard cap on parallel active tasks via asyncio.Semaphore
- Panel completely redesigned: card-style layout, richer visual hierarchy
- Panel auto-send on task arrival (auto_panel) preserved
- EDIT_INTERVAL kept at 1.5 s
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

log = logging.getLogger(__name__)

MAX_WORKERS    = 10
MAX_CONCURRENT = 5          # hard parallel task cap
EDIT_INTERVAL  = 1.5
PANEL_TTL      = 600
TASK_LINGER    = 30

# Global semaphore — limits truly concurrent task execution to 5
_task_semaphore: Optional[asyncio.Semaphore] = None


def _get_semaphore() -> asyncio.Semaphore:
    global _task_semaphore
    if _task_semaphore is None:
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    return _task_semaphore


# ─────────────────────────────────────────────────────────────
# TaskRecord
# ─────────────────────────────────────────────────────────────

_ENGINE_ICON: dict[str, str] = {
    "telegram": "📲",
    "ytdlp":    "▶️",
    "aria2":    "🧨",
    "direct":   "🔗",
    "gdrive":   "☁️",
    "ffmpeg":   "⚙️",
    "magnet":   "🧲",
    "mediafire":"📁",
}

_ENGINE_LABEL: dict[str, str] = {
    "telegram": "Telegram",
    "ytdlp":    "yt-dlp",
    "aria2":    "Aria2",
    "direct":   "Direct",
    "gdrive":   "GDrive",
    "ffmpeg":   "FFmpeg",
    "magnet":   "Aria2",
    "mediafire":"Mediafire",
}

_MODE_ICON: dict[str, str] = {
    "dl":     "📥",
    "ul":     "📤",
    "proc":   "⚙️",
    "magnet": "🧲",
    "queue":  "⏳",
}

_MODE_LABEL: dict[str, str] = {
    "dl":     "Download",
    "ul":     "Upload",
    "proc":   "Processing",
    "magnet": "Torrent",
    "queue":  "Queued",
}


@dataclass
class TaskRecord:
    tid:      str
    user_id:  int
    label:    str
    mode:     str   = "dl"
    engine:   str   = ""
    state:    str   = "⏳ Queued"
    fname:    str   = ""
    done:     int   = 0
    total:    int   = 0
    speed:    float = 0.0
    eta:      int   = 0
    elapsed:  float = 0.0
    seeds:    int   = 0
    meta_phase: bool = False
    started:  float = field(default_factory=time.time)
    finished: Optional[float] = None
    seq:      int   = 0

    _factory: Optional[Callable] = field(default=None, repr=False, compare=False)
    _dirty:   asyncio.Event      = field(default_factory=asyncio.Event, repr=False)

    def update(self, **kw) -> None:
        changed = False
        for k, v in kw.items():
            if hasattr(self, k) and k not in ("_factory", "_dirty"):
                if getattr(self, k) != v:
                    setattr(self, k, v)
                    changed = True
        self.elapsed = time.time() - self.started
        if self.state.startswith(("✅", "❌")) and self.finished is None:
            self.finished = time.time()
            changed = True
        if changed:
            try:
                self._dirty.set()
            except Exception:
                pass

    def pct(self) -> float:
        return min((self.done / self.total * 100) if self.total else 0.0, 100.0)

    @property
    def is_terminal(self) -> bool:
        return self.state.startswith(("✅", "❌"))

    @property
    def engine_icon(self) -> str:
        return _ENGINE_ICON.get(self.engine, "📦")

    @property
    def engine_lbl(self) -> str:
        return _ENGINE_LABEL.get(self.engine, self.engine or "")

    @property
    def mode_icon(self) -> str:
        return _MODE_ICON.get(self.mode, "📦")

    @property
    def mode_lbl(self) -> str:
        return _MODE_LABEL.get(self.mode, self.mode.upper())


# ─────────────────────────────────────────────────────────────
# GlobalTracker
# ─────────────────────────────────────────────────────────────

class GlobalTracker:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskRecord] = {}
        self._lock:  asyncio.Lock = asyncio.Lock()
        self._seq:   int = 0

    def new_tid(self) -> str:
        return uuid.uuid4().hex[:8].upper()

    async def register(self, record: TaskRecord) -> None:
        async with self._lock:
            self._evict()
            self._seq += 1
            record.seq = self._seq
            self._tasks[record.tid] = record
        asyncio.get_event_loop().create_task(
            runner.auto_panel(record.user_id)
        )

    async def update(self, tid: str, **kw) -> None:
        async with self._lock:
            t = self._tasks.get(tid)
            if t:
                t.update(**kw)

    async def finish(self, tid: str, success: bool = True, msg: str = "") -> None:
        state = "✅ Done" if success else f"❌ {msg or 'Failed'}"
        await self.update(tid, state=state)

    def tasks_for_user(self, user_id: int) -> list[TaskRecord]:
        self._evict_sync()
        return sorted(
            [t for t in self._tasks.values() if t.user_id == user_id],
            key=lambda t: t.seq,
        )

    def all_tasks(self) -> list[TaskRecord]:
        self._evict_sync()
        return sorted(self._tasks.values(), key=lambda t: t.seq)

    def active_tasks(self) -> list[TaskRecord]:
        return [t for t in self.all_tasks() if not t.is_terminal]

    def queued_count(self) -> int:
        """Tasks waiting for a semaphore slot."""
        return sum(1 for t in self._tasks.values() if t.state == "⏳ Queued")

    def _evict(self) -> None:
        now  = time.time()
        dead = [
            tid for tid, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for k in dead:
            self._tasks.pop(k, None)

    def _evict_sync(self) -> None:
        now  = time.time()
        dead = [
            tid for tid, t in self._tasks.items()
            if t.is_terminal and t.finished and now - t.finished > TASK_LINGER
        ]
        for k in dead:
            self._tasks.pop(k, None)


tracker = GlobalTracker()


# ─────────────────────────────────────────────────────────────
# Panel renderer  — redesigned card-style layout
# ─────────────────────────────────────────────────────────────

def _bar(pct: float, w: int = 18) -> str:
    """Segmented progress bar with a tip marker."""
    pct   = min(max(pct, 0), 100)
    filled = int(pct / 100 * w)
    empty  = w - filled
    if filled == 0:
        return "░" * w
    if filled == w:
        return "█" * w
    return "█" * (filled - 1) + "▓" + "░" * empty


def _mini_bar(pct: float, w: int = 10) -> str:
    filled = int(min(max(pct, 0), 100) / 100 * w)
    return "█" * filled + "░" * (w - filled)


def _spd_tier(bps: float) -> tuple[str, str]:
    """Returns (emoji, label) for speed tier."""
    mib = bps / (1024 * 1024)
    if mib >= 50:  return "🚀", "blazing"
    if mib >= 20:  return "⚡", "fast"
    if mib >= 5:   return "🔥", "good"
    if mib >= 1:   return "🏃", "normal"
    if mib >= 0.1: return "🐢", "slow"
    return "💤", "stalled"


def _ring(p: float) -> str:
    return "🟢" if p < 40 else ("🟡" if p < 70 else "🔴")


def _divider(label: str = "") -> str:
    if label:
        pad = max(0, 22 - len(label))
        return f"┄┄ <i>{label}</i> " + "┄" * pad
    return "━━━━━━━━━━━━━━━━━━━━━━━━━━"


async def render_panel(target_uid: Optional[int] = None) -> str:
    from services.utils import human_size, human_dur, system_stats

    tasks    = tracker.tasks_for_user(target_uid) if target_uid else tracker.all_tasks()
    active   = [t for t in tasks if not t.is_terminal]
    finished = [t for t in tasks if t.is_terminal]

    lines: list[str] = []
    now = time.time()

    # ── Header banner ─────────────────────────────────────────
    n_active  = len(active)
    n_done    = len(finished)
    n_queued  = sum(1 for t in active if t.state == "⏳ Queued")
    n_running = n_active - n_queued

    if n_active == 0 and n_done == 0:
        status_tag = "💤 Idle"
    elif n_queued and n_running:
        status_tag = f"⚙️ {n_running} running · ⏳ {n_queued} queued"
    elif n_running:
        status_tag = f"⚙️ {n_running} running"
    elif n_queued:
        status_tag = f"⏳ {n_queued} queued"
    else:
        status_tag = f"✅ {n_done} done"

    lines += [
        f"╔══ ⚡ <b>ZILONG</b>  <code>{status_tag}</code>",
        f"║  Slot capacity: <code>{MAX_CONCURRENT - n_running}/{MAX_CONCURRENT}</code> free",
        "╚" + "═" * 26,
        "",
    ]

    # ── Active task cards ─────────────────────────────────────
    for i, t in enumerate(active):
        pct = t.pct()
        bar = _bar(pct, 18)

        # Card border top
        mode_l  = t.mode_lbl
        eng_l   = t.engine_lbl
        elapsed = human_dur(int(t.elapsed)) if t.elapsed else "0s"

        if t.state == "⏳ Queued":
            lines += [
                f"┌─ #{t.seq}  ⏳ <b>QUEUED</b>  [{mode_l}]",
                f"│  📄 <code>{(t.fname or t.label)[:48]}</code>",
                f"│  Waiting for a free slot…",
                "└" + "─" * 28,
                "",
            ]
            continue

        # Running card
        label_short = (t.label[:42] + "…") if len(t.label) > 42 else t.label
        lines.append(f"┌─ #{t.seq}  {t.mode_icon} <b>{mode_l.upper()}</b>  {t.engine_icon} <code>{eng_l}</code>")

        if t.fname and t.fname != t.label:
            fname_short = (t.fname[:48] + "…") if len(t.fname) > 48 else t.fname
            lines.append(f"│  📄 <code>{fname_short}</code>")
        else:
            lines.append(f"│  📌 <code>{label_short}</code>")

        # Metadata fetch phase for magnets
        if t.meta_phase:
            lines += [
                f"│",
                f"│  🔍 <i>Fetching metadata…</i>",
                f"│  🕰 Elapsed: <code>{elapsed}</code>",
                "└" + "─" * 28,
                "",
            ]
            continue

        # Progress bar row
        lines += [
            f"│",
            f"│  <code>[{bar}]</code>  <b>{pct:.1f}%</b>",
            f"│",
        ]

        # Stats grid — 2 per line for compactness
        spd_ico, _tier = _spd_tier(t.speed)
        spd_s = (human_size(t.speed) + "/s") if t.speed else "—"
        eta_s = human_dur(t.eta) if t.eta > 0 else "—"

        lines.append(f"│  {spd_ico} <b>Speed</b>  <code>{spd_s}</code>")
        lines.append(f"│  ⏳ <b>ETA</b>    <code>{eta_s}</code>   🕰 <code>{elapsed}</code>")

        if t.total:
            lines.append(
                f"│  ✅ <b>Done</b>   <code>{human_size(t.done)}</code> / <code>{human_size(t.total)}</code>"
            )
        elif t.done:
            lines.append(f"│  ✅ <b>Done</b>   <code>{human_size(t.done)}</code>")

        if t.seeds:
            lines.append(f"│  🌱 <b>Seeds</b>  <code>{t.seeds}</code>")

        # State chip
        lines += [
            f"│  ◉ {t.state}",
            "└" + "─" * 28,
            "",
        ]

    # ── Finished list ─────────────────────────────────────────
    if finished:
        lines.append(_divider("Recent"))
        for t in finished:
            age     = now - (t.finished or now)
            age_s   = human_dur(int(age))
            fname   = (t.fname or t.label)
            short   = (fname[:36] + "…") if len(fname) > 36 else fname
            sz_s    = human_size(t.done or t.total) if (t.done or t.total) else ""
            el_s    = human_dur(int(t.elapsed)) if t.elapsed else ""
            size_part = f"  <code>{sz_s}</code>" if sz_s else ""
            time_part = f"  <i>{el_s}</i>" if el_s else ""
            lines.append(
                f"  #{t.seq} {t.state}  {t.mode_icon}  <code>{short}</code>{size_part}{time_part}  <i>{age_s} ago</i>"
            )
        lines.append("")

    # ── System stats footer ───────────────────────────────────
    stats = await system_stats()
    cpu   = stats.get("cpu", 0.0)
    rp    = stats.get("ram_pct", 0.0)
    df    = stats.get("disk_free", 0)
    dl    = stats.get("dl_speed", 0.0)
    ul    = stats.get("ul_speed", 0.0)

    lines += [
        _divider("System"),
        f"  🖥  CPU  {_ring(cpu)} <code>[{_mini_bar(cpu)}]</code> <b>{cpu:.0f}%</b>   "
        f"💾 RAM  {_ring(rp)} <code>[{_mini_bar(rp)}]</code> <b>{rp:.0f}%</b>",
        f"  💿 Disk <code>{human_size(df)} free</code>   "
        f"⬇️ <code>{human_size(dl)}/s</code>  ⬆️ <code>{human_size(ul)}/s</code>",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# LivePanel
# ─────────────────────────────────────────────────────────────

class LivePanel:
    def __init__(self, msg, uid: int) -> None:
        self._msg      = msg
        self._uid      = uid
        self._lock     = asyncio.Lock()
        self._task:    Optional[asyncio.Task] = None
        self._stopped  = False
        self._last_txt = ""
        self._wake_ev  = asyncio.Event()
        self._last_edit = 0.0
        self._last_activity = time.time()

    def wake(self) -> None:
        self._last_activity = time.time()
        self._wake_ev.set()

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._loop())

    def stop(self) -> None:
        self._stopped = True
        self._wake_ev.set()
        if self._task and not self._task.done():
            self._task.cancel()

    def is_idle(self) -> bool:
        return time.time() - self._last_activity > PANEL_TTL

    async def _edit(self) -> None:
        from services.utils import safe_edit
        from pyrogram import enums
        async with self._lock:
            try:
                text = await render_panel(self._uid)
                if text == self._last_txt:
                    return
                await safe_edit(self._msg, text, parse_mode=enums.ParseMode.HTML)
                self._last_txt  = text
                self._last_edit = time.time()
            except Exception as exc:
                log.debug("LivePanel edit uid=%d: %s", self._uid, exc)

    async def _loop(self) -> None:
        while not self._stopped:
            try:
                await asyncio.wait_for(self._wake_ev.wait(), timeout=EDIT_INTERVAL)
            except asyncio.TimeoutError:
                pass

            if self._stopped:
                break

            self._wake_ev.clear()

            since_last = time.time() - self._last_edit
            if since_last < 1.0:
                await asyncio.sleep(1.0 - since_last)

            await self._edit()

            if self.is_idle():
                log.debug("LivePanel uid=%d idle TTL — stopping", self._uid)
                self._stopped = True
                break

        try:
            tasks = tracker.tasks_for_user(self._uid)
            if tasks:
                from services.utils import safe_edit
                from pyrogram import enums
                text = await render_panel(self._uid)
                await safe_edit(
                    self._msg,
                    text + "\n\n<i>⏱ Panel closed. /status to reopen.</i>",
                    parse_mode=enums.ParseMode.HTML,
                )
        except Exception:
            pass

        if runner._panels.get(self._uid) is self:
            runner._panels.pop(self._uid, None)


# ─────────────────────────────────────────────────────────────
# TaskRunner
# ─────────────────────────────────────────────────────────────

class TaskRunner:
    def __init__(self) -> None:
        self._workers:      list[asyncio.Task]   = []
        self._panels:       dict[int, LivePanel] = {}
        self._panel_locks:  dict[int, asyncio.Lock] = {}
        self._running       = False

    def _panel_lock(self, uid: int) -> asyncio.Lock:
        if uid not in self._panel_locks:
            self._panel_locks[uid] = asyncio.Lock()
        return self._panel_locks[uid]

    def start(self) -> None:
        self._running = True
        # Pre-create the semaphore on the running event loop
        global _task_semaphore
        _task_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        loop = asyncio.get_event_loop()
        for _ in range(MAX_WORKERS):
            self._workers.append(loop.create_task(self._worker()))

    def stop(self) -> None:
        self._running = False
        for w in self._workers:
            w.cancel()
        for p in self._panels.values():
            p.stop()

    # ── Panel lifecycle ───────────────────────────────────────

    def open_panel(self, uid: int, msg, target_uid: Optional[int] = None) -> LivePanel:
        old = self._panels.get(uid)
        if old:
            old.stop()
        effective_uid = target_uid if target_uid is not None else uid
        panel = LivePanel(msg, uid=effective_uid)
        self._panels[uid] = panel
        panel.start()
        return panel

    def attach_panel(self, uid: int, msg) -> None:
        panel = self._panels.get(uid)
        if panel and not panel._stopped:
            panel.wake()
            return
        if panel:
            panel.stop()
        new_panel = LivePanel(msg, uid=uid)
        self._panels[uid] = new_panel
        new_panel.start()

    async def ensure_panel(self, uid: int, client, chat_id: int) -> None:
        async with self._panel_lock(uid):
            panel = self._panels.get(uid)
            if panel and not panel._stopped:
                panel.wake()

    async def auto_panel(self, uid: int) -> None:
        async with self._panel_lock(uid):
            old = self._panels.pop(uid, None)
            if old:
                old.stop()
                try:
                    await old._msg.delete()
                except Exception:
                    pass

            try:
                from core.session import get_client
                from pyrogram import enums
                client = get_client()
                msg = await client.send_message(
                    uid,
                    "⚡ <b>ZILONG</b>  <code>Starting…</code>",
                    parse_mode=enums.ParseMode.HTML,
                )
                new_panel = LivePanel(msg, uid=uid)
                self._panels[uid] = new_panel
                new_panel.start()
            except Exception as exc:
                log.debug("auto_panel uid=%d: %s", uid, exc)

    def close_panel(self, uid: int) -> None:
        p = self._panels.pop(uid, None)
        if p:
            p.stop()

    def _wake_panel(self, uid: int) -> None:
        p = self._panels.get(uid)
        if p:
            p.wake()

    # ── Task submission with semaphore ─────────────────────────

    async def submit(
        self,
        user_id: int,
        label:   str,
        coro_factory: Callable[[TaskRecord], Awaitable[None]],
        fname:  str = "",
        total:  int = 0,
        mode:   str = "dl",
        engine: str = "",
    ) -> TaskRecord:
        tid    = tracker.new_tid()
        record = TaskRecord(
            tid=tid, user_id=user_id, label=label,
            fname=fname, total=total, mode=mode, engine=engine,
        )
        await tracker.register(record)
        loop = asyncio.get_event_loop()
        loop.create_task(self._run_task(record, coro_factory))
        return record

    async def _run_task(self, record: TaskRecord, factory) -> None:
        sem = _get_semaphore()
        # Show queued state while waiting for a slot
        if sem.locked() or sem._value == 0:
            record.update(state="⏳ Queued")
        async with sem:
            record.update(state="⚙️ Running")
            try:
                await factory(record)
                record.update(state="✅ Done", done=record.total or record.done)
            except asyncio.CancelledError:
                record.update(state="❌ Cancelled")
            except Exception as exc:
                log.error("Task %s failed: %s", record.tid, exc)
                record.update(state=f"❌ {str(exc)[:60]}")

    async def _worker(self) -> None:
        """Legacy worker — kept so start()/stop() don't break. Does nothing now."""
        while self._running:
            await asyncio.sleep(5)


runner = TaskRunner()
