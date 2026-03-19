"""
core/bot_name.py
Persistent bot name — set once on first launch via owner chat,
stored in  data/bot_name.txt  so it survives restarts.

Priority (highest → lowest):
  1. BOT_NAME  env var  (useful for Koyeb / Docker deployments)
  2. data/bot_name.txt  (set interactively on first run)
  3. "Zilong"           (hard fallback — should never appear after setup)
"""
from __future__ import annotations

import os

_NAME_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "data", "bot_name.txt")
)
_cached: str = ""


def get_bot_name() -> str:
    """Return the stored bot name (cached after first read)."""
    global _cached
    if _cached:
        return _cached

    # 1 — env var
    env = os.environ.get("BOT_NAME", "").strip()
    if env:
        _cached = env
        return _cached

    # 2 — file
    try:
        with open(_NAME_FILE, encoding="utf-8") as fh:
            name = fh.read().strip()
        if name:
            _cached = name
            return _cached
    except FileNotFoundError:
        pass

    # 3 — fallback
    return "Zilong"


def set_bot_name(name: str) -> None:
    """Persist *name* to disk and update the in-process cache."""
    global _cached
    name = name.strip()
    _cached = name
    os.makedirs(os.path.dirname(_NAME_FILE), exist_ok=True)
    with open(_NAME_FILE, "w", encoding="utf-8") as fh:
        fh.write(name)


def is_name_configured() -> bool:
    """Return True if a name has already been saved (env var OR file)."""
    if os.environ.get("BOT_NAME", "").strip():
        return True
    return os.path.exists(_NAME_FILE)
