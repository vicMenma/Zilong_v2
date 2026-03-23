"""
services/cloudconvert_hook.py
Receives CloudConvert webhooks and auto-downloads + uploads finished files.

IMPROVEMENTS:
- Sends the webhook URL directly to the owner via Telegram on startup
- Auto-registers the URL with CloudConvert's webhook API so you never
  have to configure it manually in the CC dashboard
- Warns clearly when CC_WEBHOOK_SECRET is empty
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os

from aiohttp import web

log = logging.getLogger(__name__)

WEBHOOK_SECRET: str = ""
_runner = None
_site   = None

LISTEN_PORT = 8765


def _verify_signature(payload: bytes, signature: str) -> bool:
    if not WEBHOOK_SECRET:
        return True
    expected = hmac.new(
        WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _extract_urls(data: dict) -> list[dict]:
    results = []
    job   = data.get("job", {})
    tasks = job.get("tasks", [])
    for task in tasks:
        if task.get("operation") not in ("export/url",):
            continue
        if task.get("status") != "finished":
            continue
        files = (task.get("result") or {}).get("files", [])
        for f in files:
            url = f.get("url")
            if url:
                results.append({
                    "url":      url,
                    "filename": f.get("filename", "cloudconvert_file"),
                })
    return results


async def _process_file(url: str, filename: str, owner_id: int) -> None:
    from core.config import cfg
    from core.session import get_client, settings as _settings
    from services.downloader import smart_download
    from services.uploader import upload_file
    from services.task_runner import runner as _runner
    from services.utils import cleanup, make_tmp, smart_clean_filename, largest_file, human_size

    client = get_client()
    tmp    = make_tmp(cfg.download_dir, owner_id)

    try:
        path = await smart_download(url, tmp, user_id=owner_id, label=filename)

        if os.path.isdir(path):
            resolved = largest_file(path)
            if resolved:
                path = resolved

        if not os.path.isfile(path):
            log.error("[CC-Hook] No file after download: %s", filename)
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert download failed</b>\n"
                f"<code>{filename}</code>\n<i>No output file found.</i>",
                parse_mode="html",
            )
            cleanup(tmp)
            return

        fsize = os.path.getsize(path)

        if fsize > cfg.file_limit_b:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert file too large</b>\n"
                f"<code>{filename}</code>\n"
                f"Size: <code>{human_size(fsize)}</code>\n"
                f"Limit: <code>{human_size(cfg.file_limit_b)}</code>",
                parse_mode="html",
            )
            cleanup(tmp)
            return

        s       = await _settings.get(owner_id)
        cleaned = smart_clean_filename(os.path.basename(path))
        name, ext = os.path.splitext(cleaned)
        prefix = s.get("prefix", "").strip()
        suffix = s.get("suffix", "").strip()
        final_name = f"{prefix}{name}{suffix}{ext}"

        if final_name != os.path.basename(path):
            new_path = os.path.join(os.path.dirname(path), final_name)
            try:
                os.rename(path, new_path)
                path = new_path
            except OSError:
                pass

        sem = _runner._get_upload_sem()
        async with sem:
            from types import SimpleNamespace
            dummy_msg = SimpleNamespace(
                edit=lambda *a, **kw: asyncio.sleep(0),
                delete=lambda: asyncio.sleep(0),
                chat=SimpleNamespace(id=owner_id),
            )
            await upload_file(client, dummy_msg, path)

    except Exception as exc:
        log.error("[CC-Hook] Pipeline failed for %s: %s", filename, exc)
        try:
            await client.send_message(
                owner_id,
                f"❌ <b>CloudConvert auto-upload failed</b>\n"
                f"<code>{filename}</code>\n"
                f"<code>{str(exc)[:200]}</code>",
                parse_mode="html",
            )
        except Exception:
            pass
    finally:
        cleanup(tmp)


async def handle_cloudconvert(request: web.Request) -> web.Response:
    from core.config import cfg

    try:
        body = await request.read()

        sig = request.headers.get("CloudConvert-Signature", "")
        if WEBHOOK_SECRET and not _verify_signature(body, sig):
            log.warning("[CC-Hook] Invalid signature — rejected")
            return web.json_response({"error": "invalid signature"}, status=403)

        data  = await request.json()
        event = data.get("event", "")
        log.info("[CC-Hook] Received event: %s", event)

        if event != "job.finished":
            return web.json_response({"status": "ignored", "event": event})

        files = _extract_urls(data)
        if not files:
            log.warning("[CC-Hook] No export URLs in payload")
            return web.json_response({"status": "no_urls"})

        from core.session import get_client
        client = get_client()

        for f in files:
            asyncio.create_task(_process_file(f["url"], f["filename"], cfg.owner_id))

        log.info("[CC-Hook] Enqueued %d file(s)", len(files))
        return web.json_response({"status": "ok", "enqueued": [f["filename"] for f in files]})

    except Exception as e:
        log.error("[CC-Hook] Error: %s", e)
        return web.json_response({"error": str(e)}, status=500)


async def handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "online", "service": "cloudconvert-webhook"})


def _build_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/webhook/cloudconvert", handle_cloudconvert)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/", handle_health)
    return app


async def _register_webhook_with_cc(webhook_url: str, api_key: str, secret: str) -> bool:
    """
    Auto-register the webhook URL with CloudConvert via the API.
    This replaces any previously registered URL so you never need to
    touch the CC dashboard manually.
    Returns True on success.
    """
    import aiohttp as _aiohttp

    CC_API = "https://api.cloudconvert.com/v2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    # Events we care about
    payload = {
        "url":    webhook_url,
        "events": ["job.finished", "job.failed"],
    }
    if secret:
        payload["signing_secret"] = secret

    try:
        async with _aiohttp.ClientSession() as sess:
            # List existing webhooks and delete them first to avoid duplicates
            async with sess.get(f"{CC_API}/webhooks", headers=headers) as resp:
                if resp.status == 200:
                    existing = (await resp.json()).get("data", [])
                    for wh in existing:
                        wh_id = wh.get("id")
                        if wh_id:
                            await sess.delete(f"{CC_API}/webhooks/{wh_id}", headers=headers)
                            log.info("[CC-Hook] Deleted old webhook id=%s", wh_id)

            # Register new webhook
            async with sess.post(f"{CC_API}/webhooks", json=payload, headers=headers) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    wh_id = (data.get("data") or {}).get("id", "?")
                    log.info("[CC-Hook] Webhook registered with CC: id=%s url=%s", wh_id, webhook_url)
                    return True
                else:
                    log.warning("[CC-Hook] Webhook registration failed: %s %s",
                                resp.status, data.get("message", str(data)))
                    return False
    except Exception as exc:
        log.warning("[CC-Hook] Webhook auto-registration error: %s", exc)
        return False


async def start_webhook_server(
    port: int = LISTEN_PORT,
    ngrok_token: str = "",
) -> str:
    global _runner, _site

    app     = _build_app()
    _runner = web.AppRunner(app)
    await _runner.setup()
    _site = web.TCPSite(_runner, "0.0.0.0", port)
    await _site.start()
    log.info("[CC-Hook] Webhook server listening on port %d", port)

    if not WEBHOOK_SECRET:
        log.warning(
            "[CC-Hook] ⚠️  No CC_WEBHOOK_SECRET set — webhook accepts ALL POST requests."
        )

    public_url   = ""
    webhook_url  = ""

    if ngrok_token:
        try:
            from pyngrok import ngrok, conf
            conf.get_default().auth_token = ngrok_token
            tunnel      = ngrok.connect(port, "http")
            public_url  = tunnel.public_url
            webhook_url = f"{public_url}/webhook/cloudconvert"
            log.info("[CC-Hook] ngrok tunnel: %s", public_url)
            log.info("[CC-Hook] Webhook URL:  %s", webhook_url)

            # ── Auto-register with CloudConvert ───────────────
            from core.config import cfg
            api_key = cfg.cc_api_key or os.environ.get("CC_API_KEY", "").strip()
            if api_key:
                registered = await _register_webhook_with_cc(
                    webhook_url, api_key, WEBHOOK_SECRET
                )
                reg_status = "✅ auto-registered with CloudConvert" if registered else "⚠️ auto-registration failed — set manually in CC dashboard"
            else:
                reg_status = "⚠️ No CC_API_KEY — cannot auto-register webhook"

            # ── Notify owner via Telegram ─────────────────────
            try:
                from core.session import get_client
                client = get_client()
                await client.send_message(
                    cfg.owner_id,
                    (
                        "🌐 <b>ngrok Webhook Active</b>\n"
                        "──────────────────────\n\n"
                        f"<code>{webhook_url}</code>\n\n"
                        f"{reg_status}\n\n"
                        "<i>This URL changes every Colab restart.\n"
                        "Auto-registration handles it automatically.</i>"
                    ),
                    parse_mode="html",
                    disable_web_page_preview=True,
                )
            except Exception as notify_exc:
                log.warning("[CC-Hook] Could not notify owner: %s", notify_exc)

        except ImportError:
            log.error("[CC-Hook] pyngrok not installed — pip install pyngrok")
        except Exception as e:
            log.error("[CC-Hook] ngrok error: %s", e)
    else:
        log.info("[CC-Hook] No NGROK_TOKEN — server on localhost:%d only", port)

    return webhook_url


async def stop_webhook_server():
    global _runner, _site
    try:
        from pyngrok import ngrok
        ngrok.kill()
    except Exception:
        pass
    if _site:
        await _site.stop()
    if _runner:
        await _runner.cleanup()
