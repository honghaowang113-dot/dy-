#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-dyhonghao.top}"
EMAIL="${2:-admin@${DOMAIN}}"
APP_DIR="${APP_DIR:-/var/www/clipflow}"
APP_PORT="${APP_PORT:-${PORT:-3019}}"
SERVER_IP="${SERVER_IP:-8.156.90.229}"
SSL_DIR="/etc/nginx/ssl/${DOMAIN}"
ACME_SH="/root/.acme.sh/acme.sh"

echo "[1/5] Preparing ACME webroot..."
mkdir -p "${APP_DIR}/.well-known/acme-challenge" "${SSL_DIR}"

cat >/etc/nginx/conf.d/clipflow.conf <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name ${DOMAIN} www.${DOMAIN} ${SERVER_IP};
    client_max_body_size 300m;

    location ^~ /.well-known/acme-challenge/ {
        root ${APP_DIR};
        default_type text/plain;
        try_files \$uri =404;
    }

    location / {
        proxy_pass http://127.0.0.1:${APP_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300;
        proxy_connect_timeout 60;
        proxy_send_timeout 300;
    }
}
EOF
nginx -t
systemctl reload nginx

echo "[2/5] Installing acme.sh if needed..."
if [ ! -x "${ACME_SH}" ]; then
  curl -fsSL https://get.acme.sh | sh -s "email=${EMAIL}"
fi
"${ACME_SH}" --set-default-ca --server letsencrypt

echo "[3/5] Requesting TLS certificate for ${DOMAIN} ..."
"${ACME_SH}" --issue \
  -d "${DOMAIN}" \
  -d "www.${DOMAIN}" \
  -w "${APP_DIR}" \
  --keylength ec-256

echo "[4/5] Installing certificate and Nginx HTTPS config..."
"${ACME_SH}" --install-cert -d "${DOMAIN}" --ecc \
  --key-file "${SSL_DIR}/key.pem" \
  --fullchain-file "${SSL_DIR}/fullchain.pem" \
  --reloadcmd "systemctl reload nginx"

cat >/etc/nginx/conf.d/clipflow.conf <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name ${DOMAIN} www.${DOMAIN};

    location ^~ /.well-known/acme-challenge/ {
        root ${APP_DIR};
        default_type text/plain;
        try_files \$uri =404;
    }

    location / {
        return 301 https://\$host\$request_uri;
    }
}

server {
    listen 80;
    listen [::]:80;
    server_name ${SERVER_IP};
    client_max_body_size 300m;

    location ^~ /.well-known/acme-challenge/ {
        root ${APP_DIR};
        default_type text/plain;
        try_files \$uri =404;
    }

    location / {
        proxy_pass http://127.0.0.1:${APP_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300;
        proxy_connect_timeout 60;
        proxy_send_timeout 300;
    }
}

server {
    listen 443 ssl;
    listen [::]:443 ssl;
    http2 on;
    server_name ${DOMAIN} www.${DOMAIN};
    client_max_body_size 300m;

    ssl_certificate ${SSL_DIR}/fullchain.pem;
    ssl_certificate_key ${SSL_DIR}/key.pem;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers off;

    location ^~ /.well-known/acme-challenge/ {
        root ${APP_DIR};
        default_type text/plain;
        try_files \$uri =404;
    }

    location / {
        proxy_pass http://127.0.0.1:${APP_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 300;
        proxy_connect_timeout 60;
        proxy_send_timeout 300;
    }
}
EOF

nginx -t
systemctl reload nginx

echo "[5/5] Updating app public URL..."
if [ -f "${APP_DIR}/.env" ]; then
  if grep -q '^PUBLIC_SITE_URL=' "${APP_DIR}/.env"; then
    sed -i "s|^PUBLIC_SITE_URL=.*|PUBLIC_SITE_URL=https://${DOMAIN}|g" "${APP_DIR}/.env"
  else
    printf '\nPUBLIC_SITE_URL=https://%s\n' "${DOMAIN}" >> "${APP_DIR}/.env"
  fi
  PM2_HOME=/root/.pm2 pm2 restart clipflow --update-env || true
  PM2_HOME=/root/.pm2 pm2 save || true
fi

echo "HTTPS enabled for ${DOMAIN}"
