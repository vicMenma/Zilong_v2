# Koyeb Deployment Guide — Zilong Bot

## Why Koyeb?

| | Colab | EC2 t3.small | Koyeb nano |
|---|---|---|---|
| **Cost** | Free (12h timeout) | ~$15/mo | Free tier / ~$3/mo |
| **Uptime** | ❌ 12h max | ✅ Always on | ✅ Always on |
| **Setup** | Paste & run | SSH + bash | git push |
| **aria2c** | ✅ Installs at runtime | ✅ systemd | ⚠️ No (see note) |
| **FFmpeg** | ✅ | ✅ | ✅ (buildpack) |

> **aria2c note:** Koyeb containers don't support background daemons easily.
> Torrents/magnets will fail unless you point `ARIA2_HOST` to an external
> aria2c RPC instance (e.g. a free Oracle Cloud ARM server running aria2c).

---

## Quick Deploy

### 1. Push your repo to GitHub
```bash
git add .
git commit -m "add koyeb support"
git push
```

### 2. Create a Koyeb app
1. Go to [app.koyeb.com](https://app.koyeb.com) → **Create Service**
2. Source: **GitHub** → select `vicMenma/Zilong_multiusage`
3. Branch: `main`
4. Service type: **Worker** (no public port needed for a bot)
5. Build command: `pip install -r requirements.txt`
6. Run command: `python main.py`

### 3. Set environment variables
In **Settings → Environment variables**, add:

| Key | Value |
|---|---|
| `API_ID` | your api_id |
| `API_HASH` | your api_hash |
| `BOT_TOKEN` | your bot token |
| `OWNER_ID` | your telegram id |
| `KOYEB` | `1` |
| `PORT` | `8000` |
| `FILE_LIMIT_MB` | `2048` |
| `DOWNLOAD_DIR` | `/tmp/zilong_dl` |

Use **Secrets** for sensitive values (API_ID, API_HASH, BOT_TOKEN, OWNER_ID).

### 4. Health check
The bot automatically starts a TCP server on port 8000 when `KOYEB=1`.
Set Koyeb health check → **TCP** → port **8000**.

### 5. Instance type
- **nano** (free tier): enough for light use, ~256 MB RAM
- **micro**: recommended for active bots (~512 MB RAM, ~$3/mo)

---

## Deploy via koyeb.yaml (CLI)

```bash
# Install Koyeb CLI
curl -fsSL https://raw.githubusercontent.com/koyeb/koyeb-cli/master/install.sh | sh

# Login
koyeb login

# Deploy
koyeb app create zilong-bot --config koyeb.yaml
```

---

## Limitations on Koyeb free tier
- No persistent disk: downloads are in `/tmp` and lost on restart (fine for bots)
- No aria2c daemon: magnet/torrent downloads won't work without external aria2c RPC
- 512 MB egress/month on free tier — upgrade for heavy download bots

## External aria2c (for torrents on Koyeb)
Run aria2c on any VPS or Oracle Cloud free tier:
```bash
aria2c --enable-rpc --rpc-listen-all=true --rpc-secret=YOUR_SECRET \
       --rpc-listen-port=6800 --seed-time=0 --daemon
```
Then set in Koyeb env:
```
ARIA2_HOST=http://your-vps-ip
ARIA2_PORT=6800
ARIA2_SECRET=YOUR_SECRET
```
