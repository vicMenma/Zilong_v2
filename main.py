"""
Zilong Bot — main.py
Entry point. Loads config, builds client, registers plugins, starts.

Koyeb support: if KOYEB=1 is set in env, a lightweight HTTP health-check
server is started on port PORT (default 8000) so Koyeb's health probe passes.
"""
import asyncio
import logging
import os
import glob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("zilong.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# Remove stale sessions before import
for _f in glob.glob("*.session") + glob.glob("*.session-journal"):
    try:
        os.remove(_f)
        log.info("Removed stale session: %s", _f)
    except OSError:
        pass

from pyrogram import Client, idle
from core.config import cfg
from services.task_runner import runner


def build_client() -> Client:
    import inspect

    base_kwargs: dict = dict(
        name="ZilongBot",
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        bot_token=cfg.bot_token,
        plugins={"root": "plugins"},
        workdir="/tmp",
    )

    # concurrent_transmissions was introduced in pyrofork ≥ 2.3.40.
    # Guard against older installs so the bot never crashes on startup
    # due to a missing parameter — it simply falls back to sequential transfers.
    try:
        sig = inspect.signature(Client.__init__)
        if "concurrent_transmissions" in sig.parameters:
            base_kwargs["concurrent_transmissions"] = 4
            log.info("⚡ concurrent_transmissions=4 enabled (parallel MTProto uploads)")
        else:
            log.warning(
                "⚠️  concurrent_transmissions not supported by this pyrofork build "
                "— falling back to sequential uploads. "
                "Upgrade to pyrofork>=2.3.40 for faster upload speed."
            )
    except Exception:
        pass  # inspect failed — proceed without the param

    return Client(**base_kwargs)


async def main() -> None:
    # ── Koyeb health server ────────────────────────────────────
    if os.environ.get("KOYEB", "").strip() == "1":
        from koyeb_server import start_health_server
        port = int(os.environ.get("PORT", 8000))
        start_health_server(port)
        log.info("🌐 Koyeb health server started on port %d", port)

    client = build_client()

    import core.session as _cs
    _cs._client = client

    await client.start()
    me = await client.get_me()
    log.info("✅ @%s (id=%d) started", me.username or me.first_name, me.id)

    runner.start()
    log.info("🚀 Task runner started (max %d concurrent)", 5)

    log.info("📡 Bot is running. Press Ctrl+C to stop.")
    await idle()

    log.info("👋 Shutting down…")
    runner.stop()
    await client.stop()
    log.info("✅ Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
