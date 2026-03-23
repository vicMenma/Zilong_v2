# @title ⚡ Zilong Bot — Colab Launcher
# @markdown ## Credentials
# @markdown
# @markdown **Recommended:** Add secrets via the 🔑 icon in the left panel:
# @markdown - `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID`

API_ID    = 0      # @param {type:"integer"}
API_HASH  = ""     # @param {type:"string"}
BOT_TOKEN = ""     # @param {type:"string"}
OWNER_ID  = 0      # @param {type:"integer"}

FILE_LIMIT_MB = 2048          # @param {type:"integer"}
LOG_CHANNEL   = "@zilong_dump" # @param {type:"string"}

# CloudConvert auto-upload (optional)
NGROK_TOKEN       = ""  # @param {type:"string"}
CC_WEBHOOK_SECRET = ""  # @param {type:"string"}

# CloudConvert hardsub API key (for /hardsub command)
CC_API_KEY        = ""  # @param {type:"string"}

import os, sys, subprocess, shutil, time, glob
from datetime import datetime

GITHUB_TOKEN = ""   # @param {type:"string"}  ← paste PAT here, or use 🔑 secret
REPO_OWNER   = "vicMenma"
REPO_NAME    = "Zilong_v2"
BASE_DIR     = "/content/zilong"


def _log(level: str, msg: str):
    icons = {"INFO": "ℹ️", "OK": "✅", "WARN": "⚠️", "ERR": "❌", "STEP": "🔧"}
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {icons.get(level, '')} {msg}", flush=True)


def _secret(name: str) -> str:
    try:
        from google.colab import userdata
        val = userdata.get(name)
        if val:
            return str(val).strip()
    except Exception:
        pass
    return os.environ.get(name, "").strip()


def _resolve_channel(val) -> str:
    """
    Return the channel value as a clean string.
    Accepts:
      - integer  → "123456789"
      - "@username" → "@username"  (kept as-is for Pyrogram)
      - "-100123456789" → "-100123456789"
      - "" / 0   → "0"  (disabled)
    """
    s = str(val).strip()
    if not s or s == "0":
        return "0"
    return s


print("⚡ Zilong Bot — Colab Launcher (Zilong_v2)")
print("─" * 50)
_log("STEP", "Resolving credentials…")

if not API_ID:
    try: API_ID = int(_secret("API_ID"))
    except: API_ID = 0
if not API_HASH:  API_HASH  = _secret("API_HASH")
if not BOT_TOKEN: BOT_TOKEN = _secret("BOT_TOKEN")
if not OWNER_ID:
    try: OWNER_ID = int(_secret("OWNER_ID"))
    except: OWNER_ID = 0
if not FILE_LIMIT_MB:
    try: FILE_LIMIT_MB = int(_secret("FILE_LIMIT_MB") or 2048)
    except: FILE_LIMIT_MB = 2048

# LOG_CHANNEL: supports @username, numeric ID, or 0 (disabled)
if not LOG_CHANNEL or str(LOG_CHANNEL).strip() in ("", "0"):
    _lc_raw = _secret("LOG_CHANNEL") or "0"
    LOG_CHANNEL = _resolve_channel(_lc_raw)
else:
    LOG_CHANNEL = _resolve_channel(LOG_CHANNEL)

if not GITHUB_TOKEN: GITHUB_TOKEN = _secret("GITHUB_TOKEN")
if not NGROK_TOKEN:
    NGROK_TOKEN = _secret("NGROK_TOKEN") or _secret("NGROK_AUTHTOKEN")
if not CC_WEBHOOK_SECRET: CC_WEBHOOK_SECRET = _secret("CC_WEBHOOK_SECRET")
if not CC_API_KEY:        CC_API_KEY        = _secret("CC_API_KEY")

errors = []
if not API_ID:        errors.append("API_ID is required")
if not API_HASH:      errors.append("API_HASH is required")
if not BOT_TOKEN:     errors.append("BOT_TOKEN is required")
if not OWNER_ID:      errors.append("OWNER_ID is required")
if not GITHUB_TOKEN:  errors.append("GITHUB_TOKEN is required (repo is private)")
if errors:
    print()
    for e in errors: print(f"  ❌ {e}")
    print()
    raise SystemExit("Fill in credentials and run again.")

_log("OK", f"Credentials loaded  (API_ID={API_ID}, OWNER_ID={OWNER_ID})")
_log("OK", f"Log channel: {LOG_CHANNEL if LOG_CHANNEL != '0' else 'disabled'}")
if NGROK_TOKEN:
    _log("OK", "CloudConvert webhook enabled (NGROK_TOKEN set)")
else:
    _log("WARN", "NGROK_TOKEN is empty after secret resolution — webhook will be disabled")
if CC_API_KEY:
    _log("OK", "CloudConvert hardsub enabled (CC_API_KEY set)")

_log("STEP", "Installing system packages…")
subprocess.run(
    "apt-get update -qq && "
    "apt-get install -y -qq ffmpeg aria2 mediainfo p7zip-full unrar 2>/dev/null",
    shell=True, capture_output=True,
)
_log("OK", "System packages ready")

_log("STEP", "Cloning repository…")
if os.path.exists(BASE_DIR):
    shutil.rmtree(BASE_DIR)
REPO_URL = f"https://{GITHUB_TOKEN}@github.com/{REPO_OWNER}/{REPO_NAME}.git"
r = subprocess.run(["git", "clone", "--depth=1", REPO_URL, BASE_DIR],
                   capture_output=True, text=True)
if r.returncode != 0:
    # Mask token in error output so it doesn't leak into logs
    err_clean = r.stderr.replace(GITHUB_TOKEN, "***")
    raise SystemExit(f"❌ Clone failed:\n{err_clean[:300]}")
_log("OK", f"Cloned {REPO_OWNER}/{REPO_NAME} to {BASE_DIR}")

_log("STEP", "Installing Python packages…")
# Remove stock pyrogram before installing pyrofork — both expose the same
# `pyrogram` namespace. If both are installed, stock pyrogram wins and
# pyrofork-specific internals won't be available.
subprocess.run(
    [sys.executable, "-m", "pip", "uninstall", "-q", "-y", "pyrogram"],
    capture_output=True,
)
subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q",
     "-r", f"{BASE_DIR}/requirements.txt"],
    check=True,
)
_log("OK", "Python packages installed")

_log("STEP", "Starting aria2c daemon…")
subprocess.Popen(
    "aria2c --enable-rpc --rpc-listen-all=true --rpc-allow-origin-all "
    "--max-connection-per-server=16 --split=16 --seed-time=0 --daemon 2>/dev/null",
    shell=True,
)
time.sleep(2)
_log("OK", "aria2c started")

# ── Patch config.py to support string LOG_CHANNEL (@username / numeric) ──────
# core/config.py defines log_channel as int via _int_env(), which silently
# returns 0 for "@username" strings. We patch it to keep the raw string value
# so Pyrogram can forward to both @username channels and numeric IDs.
_log("STEP", "Patching config.py for string LOG_CHANNEL support…")
try:
    import re as _re
    _cfg_path = os.path.join(BASE_DIR, "core", "config.py")
    _cfg_src  = open(_cfg_path).read()
    _OLD_LC = (
        "    log_channel: int = field(default_factory=lambda:\n"
        "        _int_env(\"LOG_CHANNEL\", 0))"
    )
    _NEW_LC = (
        "    log_channel: str = field(default_factory=lambda:\n"
        "        os.environ.get(\"LOG_CHANNEL\", \"0\").strip())"
    )
    if _OLD_LC in _cfg_src:
        _cfg_src = _cfg_src.replace(_OLD_LC, _NEW_LC, 1)
        open(_cfg_path, "w").write(_cfg_src)
        _log("OK", "config.py patched — log_channel now accepts @username")
    else:
        _log("OK", "config.py patch already applied or not needed")
except Exception as _pe:
    _log("WARN", f"config.py patch failed (non-critical): {_pe}")

# ── Patch load_dotenv to use override=True ───────────────────────────────────
# core/config.py calls load_dotenv() with the default override=False.
# In Colab, stale empty env vars (e.g. NGROK_TOKEN="") from a previous
# session survive into the subprocess environment and silently block the
# real values from .env. override=True makes the .env file always win.
_log("STEP", "Patching config.py load_dotenv → override=True…")
try:
    _cfg_path2 = os.path.join(BASE_DIR, "core", "config.py")
    _cfg2 = open(_cfg_path2).read()
    if "load_dotenv()" in _cfg2:
        _cfg2 = _cfg2.replace("load_dotenv()", "load_dotenv(override=True)", 1)
        open(_cfg_path2, "w").write(_cfg2)
        _log("OK", "load_dotenv patched — .env always takes priority")
    else:
        _log("OK", "load_dotenv patch already applied or not needed")
except Exception as _lde:
    _log("WARN", f"load_dotenv patch failed (non-critical): {_lde}")

# ── Patch uploader.py to use log_channel as string when forwarding ────────────
_log("STEP", "Patching uploader.py for string log_channel…")
try:
    _up_path = os.path.join(BASE_DIR, "services", "uploader.py")
    _up_src  = open(_up_path).read()
    _OLD_FWD = "        if cfg.log_channel and sent:\n            try:\n                await sent.forward(cfg.log_channel)"
    _NEW_FWD = "        if cfg.log_channel and cfg.log_channel != \"0\" and sent:\n            try:\n                await sent.forward(cfg.log_channel)"
    if _OLD_FWD in _up_src:
        _up_src = _up_src.replace(_OLD_FWD, _NEW_FWD, 1)
        open(_up_path, "w").write(_up_src)
        _log("OK", "uploader.py patched — forward guard updated")
    else:
        _log("OK", "uploader.py patch already applied or not needed")
except Exception as _upe:
    _log("WARN", f"uploader.py patch failed (non-critical): {_upe}")

# ── Apply thumbnail duration fix to uploader.py ───────────────────────────────
_log("STEP", "Applying thumbnail duration fix to uploader.py…")
try:
    import re as _re
    _up = os.path.join(BASE_DIR, "services", "uploader.py")
    _src = open(_up).read()
    _c = 0
    for _old, _new in [
        ('"-v", "error",',                               '"-v", "quiet",'),
        ('"-show_entries", "format=duration",',          '"-show_streams", "-show_format",'),
        ('"-of", "default=noprint_wrappers=1:nokey=1",', '"-of", "json",'),
    ]:
        if _old in _src:
            _src = _src.replace(_old, _new, 1); _c += 1
    _OLD_DUR = 'duration = float(out_b.decode().strip() or "0")'
    _NEW_DUR = (
        "import json as _jj\n"
        "        _jdata = _jj.loads(out_b.decode(errors='replace') or '{}')\n"
        "        duration = 0\n"
        "        for _jst in _jdata.get('streams', []):\n"
        "            if _jst.get('codec_type') == 'video':\n"
        "                try: duration = int(float(_jst.get('duration', 0) or 0))\n"
        "                except: pass\n"
        "                for _jk in ('DURATION', 'DURATION-eng', 'DURATION-jpn'):\n"
        "                    _jv = (_jst.get('tags') or {}).get(_jk, '')\n"
        "                    if not duration and _jv and ':' in str(_jv):\n"
        "                        try:\n"
        "                            _jp = str(_jv).split(':')\n"
        "                            duration = int(float(_jp[0]))*3600+int(float(_jp[1]))*60+int(float(_jp[2].split('.')[0]))\n"
        "                        except: pass\n"
        "                    if duration: break\n"
        "                break\n"
        "        if not duration:\n"
        "            try: duration = int(float(_jdata.get('format', {}).get('duration') or 0))\n"
        "            except: pass"
    )
    if _OLD_DUR in _src:
        _src = _src.replace(_OLD_DUR, _NEW_DUR, 1); _c += 1
    if _c:
        open(_up, "w").write(_src)
        _log("OK", f"Thumbnail fix applied ({_c} replacements)")
    else:
        _log("OK", "Thumbnail fix already present")
except Exception as _e:
    _log("WARN", f"Patch failed: {_e}")

env_lines = [
    f"API_ID={API_ID}",
    f"API_HASH={API_HASH}",
    f"BOT_TOKEN={BOT_TOKEN}",
    f"OWNER_ID={OWNER_ID}",
    f"FILE_LIMIT_MB={FILE_LIMIT_MB}",
    f"LOG_CHANNEL={LOG_CHANNEL}",
    "DOWNLOAD_DIR=/tmp/zilong_dl",
    "ARIA2_HOST=http://localhost",
    "ARIA2_PORT=6800",
    "ARIA2_SECRET=",
    # ── CloudConvert webhook ──────────────────────────────────────────────
    f"NGROK_TOKEN={NGROK_TOKEN}",
    f"CC_WEBHOOK_SECRET={CC_WEBHOOK_SECRET}",
    # ── CloudConvert hardsub API key ──────────────────────────────────────
    f"CC_API_KEY={CC_API_KEY}",
    # ── Upload speed tuning ───────────────────────────────────────────────
    # UPLOAD_CONCURRENCY: 1 = sequential uploads (safe for Colab bandwidth)
    "UPLOAD_CONCURRENCY=1",
    # BOT_WORKERS: pyrofork dispatcher thread pool
    "BOT_WORKERS=16",
    # UPLOAD_PARTS_PARALLEL: concurrent 512KB MTProto parts per upload
    "UPLOAD_PARTS_PARALLEL=16",
]
for optional in ("ADMINS", "GDRIVE_SA_JSON", "ARIA2_SECRET"):
    val = _secret(optional)
    if val:
        env_lines.append(f"{optional}={val}")

with open(f"{BASE_DIR}/.env", "w") as f:
    f.write("\n".join(env_lines))

for sf in glob.glob(os.path.join(BASE_DIR, "*.session*")):
    try: os.remove(sf)
    except OSError: pass

_log("OK", "Environment configured (.env written)")

os.chdir(BASE_DIR)

# ── Keep Colab alive ──────────────────────────────────────────
_log("STEP", "Activating Colab keep-alive…")
try:
    from IPython.display import display, Javascript
    display(Javascript('''
    function ColabKeepAlive() {
        document.querySelector("#top-toolbar .colab-connect-button")?.click();
        document.querySelector("colab-connect-button")?.shadowRoot
            ?.querySelector("#connect")?.click();
        document.querySelector("#ok")?.click();
    }
    setInterval(ColabKeepAlive, 60000);
    console.log("Colab keep-alive: clicking connect every 60s");
    '''))
    _log("OK", "JS keep-alive injected (clicks connect every 60s)")
except Exception:
    _log("WARN", "Not in Colab notebook — JS keep-alive skipped")

import threading

def _heartbeat():
    """Print a silent heartbeat every 5 min to keep stdout active."""
    while True:
        time.sleep(300)
        ts = datetime.now().strftime("%H:%M")
        print(f"[{ts}] 💓", end="", flush=True)

_hb = threading.Thread(target=_heartbeat, daemon=True)
_hb.start()
_log("OK", "Heartbeat thread started (every 5 min)")

_log("OK", "Starting bot…\n" + "─" * 50)

MAX_RESTARTS  = 50
restart_count = 0

while restart_count < MAX_RESTARTS:
    t_start = datetime.now()
    # Inject ALL resolved vars directly into the subprocess environment.
    # This bypasses python-dotenv's default override=False behaviour, which
    # silently keeps stale empty values from a previous Colab session and
    # causes secrets like NGROK_TOKEN to appear missing even when set.
    _injected_env = {
        **os.environ,
        "PYTHONUNBUFFERED":     "1",
        "API_ID":               str(API_ID),
        "API_HASH":             API_HASH,
        "BOT_TOKEN":            BOT_TOKEN,
        "OWNER_ID":             str(OWNER_ID),
        "FILE_LIMIT_MB":        str(FILE_LIMIT_MB),
        "LOG_CHANNEL":          LOG_CHANNEL,
        "NGROK_TOKEN":          NGROK_TOKEN,
        "CC_WEBHOOK_SECRET":    CC_WEBHOOK_SECRET,
        "CC_API_KEY":           CC_API_KEY,
        "DOWNLOAD_DIR":         "/tmp/zilong_dl",
        "ARIA2_HOST":           "http://localhost",
        "ARIA2_PORT":           "6800",
        "ARIA2_SECRET":         "",
        "UPLOAD_CONCURRENCY":   "1",
        "BOT_WORKERS":          "16",
        "UPLOAD_PARTS_PARALLEL":"16",
    }
    # Optional secrets — only inject if non-empty
    for _opt in ("ADMINS", "GDRIVE_SA_JSON"):
        _v = _secret(_opt)
        if _v:
            _injected_env[_opt] = _v

    proc = subprocess.Popen(
        [sys.executable, "-u", "main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, bufsize=1,
        env=_injected_env,
    )
    for line in proc.stdout:
        print(line, end="", flush=True)
    proc.wait()

    elapsed = (datetime.now() - t_start).seconds
    if proc.returncode == 0:
        _log("OK", "Bot stopped cleanly.")
        break

    # Long-running crash → not a startup bug, reset counter
    if elapsed > 300:
        restart_count = 0

    restart_count += 1
    _log("WARN", f"Crashed (exit={proc.returncode}) after {elapsed}s  [{restart_count}/{MAX_RESTARTS}]")
    if restart_count >= MAX_RESTARTS:
        _log("ERR", "Too many restarts — stopping.")
        break
    wait = min(5 * restart_count, 30)
    _log("WARN", f"Restarting in {wait}s…")
    time.sleep(wait)
