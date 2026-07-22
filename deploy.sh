#!/usr/bin/env bash
# Deploy the GCC Real Estate Intelligence Bot as a systemd service + daily cron.
# Run from the repo root. Override APP_DIR / PYTHON / SERVICE_USER as needed.
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "$0")" && pwd)}"
PYTHON="${PYTHON:-$APP_DIR/.venv/bin/python3}"
SERVICE_USER="${SERVICE_USER:-$(id -un)}"
BOT="$APP_DIR/gcc_re_intel_bot.py"

echo "=== Deploying GCC RE Intelligence Bot from $APP_DIR ==="

# 1. Install dependencies into the project venv
"$PYTHON" -m pip install -r "$APP_DIR/requirements.txt" -q
echo "Dependencies installed"

# 2. Dry-run every source (no Telegram traffic, no LLM calls)
"$PYTHON" "$BOT" --test
echo "Source test complete"

# 3. systemd unit for the interactive bot loop
sudo tee /etc/systemd/system/gcc-re-intel-bot.service > /dev/null <<EOF
[Unit]
Description=GCC Real Estate Intelligence Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${PYTHON} ${BOT} --bot
Restart=always
RestartSec=15
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# 4. Daily scrape at 03:00 UTC (07:00 Gulf Standard Time)
CRON="0 3 * * * ${PYTHON} ${BOT} --scrape"
( crontab -l 2>/dev/null | grep -v gcc_re_intel_bot; echo "$CRON" ) | crontab -

# 5. (Re)start the service
sudo systemctl daemon-reload
sudo systemctl enable --now gcc-re-intel-bot.service

echo ""
echo "=== Deployment complete ==="
echo "Service: $(systemctl is-active gcc-re-intel-bot.service)"
echo ""
echo "Commands:"
echo "  $PYTHON $BOT --test    # dry-run all sources"
echo "  $PYTHON $BOT --scrape  # manual morning run"
