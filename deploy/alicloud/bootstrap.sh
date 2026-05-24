#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${1:-https://github.com/honghaowang113-dot/dy-.git}"
APP_DIR="${2:-/var/www/clipflow}"
DOMAIN="${3:-dyhonghao.top}"
PUBLIC_URL="${PUBLIC_URL:-http://${DOMAIN}}"
APP_PORT="${PORT:-3019}"

echo "[1/8] Installing base packages..."
sudo apt-get update -y
sudo apt-get install -y curl git nginx ffmpeg ca-certificates gnupg

echo "[2/8] Installing Node.js 20.x..."
if ! command -v node >/dev/null 2>&1; then
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi

echo "[3/8] Installing PM2..."
if ! command -v pm2 >/dev/null 2>&1; then
  sudo npm install -g pm2 --registry=https://registry.npmmirror.com
fi

echo "[4/8] Pulling project..."
sudo mkdir -p "${APP_DIR}"
sudo chown -R "$USER":"$USER" "${APP_DIR}"
if [ ! -d "${APP_DIR}/.git" ]; then
  git clone "${REPO_URL}" "${APP_DIR}"
else
  git -C "${APP_DIR}" fetch origin
  git -C "${APP_DIR}" checkout master
  git -C "${APP_DIR}" pull --ff-only origin master
fi

echo "[5/8] Installing dependencies..."
cd "${APP_DIR}"
npm config set registry https://registry.npmmirror.com
if [ -f package-lock.json ]; then
  npm ci --omit=dev --registry=https://registry.npmmirror.com
else
  npm install --omit=dev --registry=https://registry.npmmirror.com
fi

echo "[6/8] Preparing environment..."
if [ ! -f .env ]; then
  cp .env.example .env
fi
set_env() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" .env; then
    sed -i "s|^${key}=.*|${key}=${value}|g" .env
  else
    printf '\n%s=%s\n' "${key}" "${value}" >> .env
  fi
}
set_env NODE_ENV production
set_env PORT "${APP_PORT}"
set_env PUBLIC_SITE_URL "${PUBLIC_URL}"
set_env REDIRECT_FAST_BUDGET_MS 1200
set_env REDIRECT_TIMEOUT_MS 4500
set_env PROVIDER_REQUEST_TIMEOUT_MS 7000
set_env PARSE_PROVIDER_CONCURRENCY 4
set_env MAX_CANDIDATE_URLS 2
set_env PARSE_CACHE_TTL_MINUTES 60

echo "[7/8] Configuring Nginx..."
cat > /tmp/clipflow.nginx.conf <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name ${DOMAIN} www.${DOMAIN};

    client_max_body_size 300m;

    location / {
        proxy_pass http://127.0.0.1:${APP_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
EOF
sudo cp /tmp/clipflow.nginx.conf /etc/nginx/sites-available/clipflow
sudo ln -sf /etc/nginx/sites-available/clipflow /etc/nginx/sites-enabled/clipflow
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl restart nginx
sudo systemctl enable nginx

echo "[8/8] Starting app with PM2..."
if [ -f ecosystem.config.cjs ]; then
  PM2_HOME=/root/.pm2 pm2 start ecosystem.config.cjs --env production --update-env
else
  PM2_HOME=/root/.pm2 PORT="${APP_PORT}" pm2 start server.js --name clipflow --update-env
fi
PM2_HOME=/root/.pm2 pm2 save
sudo env PATH="$PATH" PM2_HOME=/root/.pm2 pm2 startup systemd -u root --hp /root >/tmp/pm2-startup.sh
sudo bash /tmp/pm2-startup.sh || true

echo
echo "Bootstrap done."
echo "Verify:"
echo "  PM2_HOME=/root/.pm2 pm2 status"
echo "  curl -s http://127.0.0.1:${APP_PORT}/api/health | head -c 1200; echo"
echo "  curl -I http://${DOMAIN}/ | head"
echo "Next, after HTTP works, run SSL script:"
echo "bash ${APP_DIR}/deploy/alicloud/enable-https.sh ${DOMAIN}"
