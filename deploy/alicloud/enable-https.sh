#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-dyqushuiyin.top}"
EMAIL="${2:-admin@${DOMAIN}}"

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

echo "HTTPS enabled for ${DOMAIN}"
