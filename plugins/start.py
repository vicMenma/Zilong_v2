"""
plugins/start.py
/start  /help  /settings  /info
"""
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from core.config import cfg
from core.session import users, settings
from services.utils import human_size

HELP_TEXT = """⚡ <b>ZILONG BOT — Features</b>

📹 <b>Video processing</b>
› Trim · Split · Merge · Rename
› Stream Extractor / Mapper / Remover
› Auto + Manual Screenshots · Sample Clip
› Convert · Optimize (CRF) · Metadata
› Subtitle mux/burn · Audio-Video merge

🎵 <b>Audio</b>
› Extract · Remove · Convert
› Formats: mp3 aac m4a opus ogg flac wav wma ac3

🔗 <b>Downloads</b>
› HTTP/HTTPS direct links
› YouTube · Instagram · TikTok · Twitter and 1000+ sites
› Google Drive · Mediafire
› Torrents &amp; Magnet links via aria2c

📦 <b>Archives</b>
› Extract: zip rar 7z tar.gz
› Create: zip 7z tar.gz

📨 <b>Forward</b> without forward tag

⚙️ /settings · /info · /status
📊 /status — live dashboard
📋 /log — last 50 log lines (admin)"""


def _start_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Help",        callback_data="cb_help"),
         InlineKeyboardButton("⚙️ Settings",    callback_data="cb_settings")],
        [InlineKeyboardButton("👤 My Account",  callback_data="cb_account")],
    ])


def _settings_kb(s: dict) -> InlineKeyboardMarkup:
    rename      = s.get("rename_file", False)
    custom_name = s.get("custom_name", "").strip()
    mode        = "📄 Document" if s.get("upload_mode") == "document" else "📁 Auto"

    if rename and custom_name:
        rename_label = f"✏️ Rename: ✅ → {custom_name[:20]}"
    elif rename:
        rename_label = "✏️ Auto-Rename: ✅ (send a name)"
    else:
        rename_label = "✏️ Auto-Rename: ❌ OFF"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(rename_label,               callback_data="st_rename")],
        [InlineKeyboardButton(f"📤 Upload Mode: {mode}",  callback_data="st_mode")],
        [InlineKeyboardButton("🖼️ Set Thumbnail",          callback_data="st_thumb"),
         InlineKeyboardButton("🗑️ Clear Thumbnail",        callback_data="st_clearthumb")],
        [InlineKeyboardButton("❌ Close",                   callback_data="st_close")],
    ])


def _back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="cb_start")],
    ])


def _welcome(name: str) -> str:
    return (
        f"⚡ <b>ZILONG BOT</b>\n\n"
        f"Hello <b>{name}</b>!\n\n"
        "Send me a link, video, or audio file and I'll handle the rest.\n\n"
        "📥 Download from any URL\n"
        "🧲 Torrents &amp; magnet links\n"
        "🎬 Full video toolkit\n"
        "📦 Archive management\n\n"
        "<i>Tap <b>Help</b> to see everything.</i>"
    )


@Client.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, msg: Message):
    uid  = msg.from_user.id
    name = msg.from_user.first_name or "there"
    await users.register(uid, name)
    await msg.reply(_welcome(name), reply_markup=_start_kb(),
                    parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("help") & filters.private)
async def cmd_help(client: Client, msg: Message):
    await msg.reply(HELP_TEXT, parse_mode=enums.ParseMode.HTML,
                    disable_web_page_preview=True)


@Client.on_message(filters.command("settings") & filters.private)
async def cmd_settings(client: Client, msg: Message):
    s = await settings.get(msg.from_user.id)
    await msg.reply("⚙️ <b>Settings</b>",
                    reply_markup=_settings_kb(s),
                    parse_mode=enums.ParseMode.HTML)


@Client.on_message(filters.command("info") & filters.private)
async def cmd_info(client: Client, msg: Message):
    u    = msg.from_user
    uid  = u.id
    is_admin = uid in cfg.admins
    limit    = human_size(cfg.file_limit_b)
    await msg.reply(
        f"👤 <b>Your Account</b>\n\n"
        f"<b>ID:</b> <code>{uid}</code>\n"
        f"<b>Name:</b> {u.first_name} {u.last_name or ''}\n"
        f"<b>Username:</b> @{u.username or 'none'}\n"
        f"<b>File limit:</b> <code>{limit}</code>\n"
        f"<b>Admin:</b> {'✅' if is_admin else '❌'}",
        parse_mode=enums.ParseMode.HTML,
    )


# ── Callback handlers ─────────────────────────────────────────

@Client.on_callback_query(filters.regex("^cb_start$"))
async def cq_start(client: Client, cb: CallbackQuery):
    name = cb.from_user.first_name or "there"
    await cb.message.edit(_welcome(name), reply_markup=_start_kb(),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer()


@Client.on_callback_query(filters.regex("^cb_help$"))
async def cq_help(client: Client, cb: CallbackQuery):
    await cb.message.edit(HELP_TEXT, parse_mode=enums.ParseMode.HTML,
                          reply_markup=_back_kb())
    await cb.answer()


@Client.on_callback_query(filters.regex("^cb_settings$"))
async def cq_settings(client: Client, cb: CallbackQuery):
    s = await settings.get(cb.from_user.id)
    await cb.message.edit("⚙️ <b>Settings</b>",
                          reply_markup=_settings_kb(s),
                          parse_mode=enums.ParseMode.HTML)
    await cb.answer()


@Client.on_callback_query(filters.regex("^cb_account$"))
async def cq_account(client: Client, cb: CallbackQuery):
    u      = cb.from_user
    uid    = u.id
    limit  = human_size(cfg.file_limit_b)
    await cb.message.edit(
        f"👤 <b>My Account</b>\n\n"
        f"<b>Name:</b> {u.first_name} {u.last_name or ''}\n"
        f"<b>ID:</b> <code>{uid}</code>\n"
        f"<b>Username:</b> @{u.username or 'none'}\n"
        f"<b>File limit:</b> <code>{limit}</code>\n"
        f"<b>Admin:</b> {'✅' if uid in cfg.admins else '❌'}",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=_back_kb(),
    )
    await cb.answer()


@Client.on_callback_query(filters.regex("^st_rename$"))
async def cq_st_rename(client: Client, cb: CallbackQuery):
    uid = cb.from_user.id
    s   = await settings.get(uid)
    new = not s.get("rename_file", False)
    await settings.update(uid, {"rename_file": new})

    if new:
        # Mark user as waiting for a rename template reply
        _RENAME_WAITING.add(uid)
        await cb.answer("Auto-Rename ON ✅ — send me the name to use")
        await cb.message.edit(
            "✏️ <b>Auto-Rename is ON</b>\n\n"
            "Reply with the <b>filename</b> to use for all future downloads.\n"
            "<i>The original file extension is kept automatically.</i>\n\n"
            "Example: <code>My Series S01E01</code>\n\n"
            "<i>Send /cancel to cancel.</i>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        # Turn OFF — clear saved name too
        await settings.update(uid, {"custom_name": ""})
        _RENAME_WAITING.discard(uid)
        s["rename_file"] = False
        s["custom_name"] = ""
        await cb.message.edit_reply_markup(_settings_kb(s))
        await cb.answer("Auto-Rename OFF ❌")


@Client.on_callback_query(filters.regex("^st_mode$"))
async def cq_st_mode(client: Client, cb: CallbackQuery):
    s   = await settings.get(cb.from_user.id)
    new = "document" if s.get("upload_mode") != "document" else "auto"
    await settings.update(cb.from_user.id, {"upload_mode": new})
    s["upload_mode"] = new
    await cb.message.edit_reply_markup(_settings_kb(s))
    await cb.answer(f"Mode: {new} ✅")


@Client.on_callback_query(filters.regex("^st_thumb$"))
async def cq_st_thumb(client: Client, cb: CallbackQuery):
    await cb.message.edit(
        "🖼️ <b>Set Thumbnail</b>\n\nSend a photo — it will be used for all uploads.",
        parse_mode=enums.ParseMode.HTML,
    )
    await cb.answer()


@Client.on_callback_query(filters.regex("^st_clearthumb$"))
async def cq_st_clear(client: Client, cb: CallbackQuery):
    await settings.update(cb.from_user.id, {"thumb_id": None})
    await cb.answer("Thumbnail cleared ✅", show_alert=True)


@Client.on_callback_query(filters.regex("^st_close$"))
async def cq_st_close(client: Client, cb: CallbackQuery):
    await cb.message.delete()
    await cb.answer()


# ── Rename name collector ─────────────────────────────────────────────────────
# Users who tapped "Auto-Rename ON" are placed in this set.
# The next plain-text message they send is captured as their rename template.
_RENAME_WAITING: set[int] = set()


@Client.on_message(filters.private & filters.text & ~filters.command(
    ["start","help","settings","info","status","log","restart","broadcast",
     "admin","ban_user","unban_user","banned_list","cancel",
     "show_thumb","del_thumb","json_formatter","bulk_url"]
), group=8)
async def rename_name_collector(client: Client, msg: Message):
    uid = msg.from_user.id
    if uid not in _RENAME_WAITING:
        return

    raw_name = msg.text.strip()

    # /cancel → abort rename setup
    if raw_name.lower() in ("/cancel", "cancel"):
        _RENAME_WAITING.discard(uid)
        await settings.update(uid, {"rename_file": False, "custom_name": ""})
        await msg.reply("❌ Auto-Rename cancelled.")
        msg.stop_propagation()
        return

    # Strip any extension the user accidentally included
    import os as _os
    name_no_ext = _os.path.splitext(raw_name)[0].strip()
    if not name_no_ext:
        await msg.reply("❌ Name cannot be empty. Try again or send /cancel.")
        return

    _RENAME_WAITING.discard(uid)
    await settings.update(uid, {"custom_name": name_no_ext})

    s = await settings.get(uid)
    await msg.reply(
        f"✅ <b>Auto-Rename set!</b>\n\n"
        f"Every downloaded file will be renamed to:\n"
        f"<code>{name_no_ext}.[original_ext]</code>\n\n"
        f"<i>Tap Auto-Rename again in /settings to change or turn off.</i>",
        parse_mode=enums.ParseMode.HTML,
    )
    msg.stop_propagation()

