#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-dyhonghao.top}"
EMAIL="${2:-admin@${DOMAIN}}"
APP_DIR="${APP_DIR:-/var/www/clipflow}"

echo "[1/3] Installing certbot..."
sudo apt-get update -y
sudo apt-get install -y certbot python3-certbot-nginx

echo "[2/3] Requesting TLS certificate for ${DOMAIN} ..."
sudo certbot --nginx \
  -d "${DOMAIN}" \
  -d "www.${DOMAIN}" \
  --non-interactive \
  --agree-tos \
  -m "${EMAIL}" \
  --redirect

echo "[3/3] Verifying certbot timer..."
sudo systemctl enable certbot.timer
sudo systemctl start certbot.timer
sudo certbot renew --dry-run

if [ -f "${APP_DIR}/.env" ]; then
  if grep -q '^PUBLIC_SITE_URL=' "${APP_DIR}/.env"; then
    sudo sed -i "s|^PUBLIC_SITE_URL=.*|PUBLIC_SITE_URL=https://${DOMAIN}|g" "${APP_DIR}/.env"
  else
    printf '\nPUBLIC_SITE_URL=https://%s\n' "${DOMAIN}" | sudo tee -a "${APP_DIR}/.env" >/dev/null
  fi
  PM2_HOME=/root/.pm2 pm2 restart clipflow --update-env || true
fi

echo "HTTPS enabled for ${DOMAIN}"
