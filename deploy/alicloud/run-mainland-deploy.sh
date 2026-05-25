#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/var/www/clipflow}"
REPO="${REPO:-https://github.com/honghaowang113-dot/dy-.git}"
DOMAIN="${DOMAIN:-dyhonghao.top}"
APP_PORT="${APP_PORT:-${PORT:-3019}}"
PUBLIC_URL="${PUBLIC_URL:-http://${DOMAIN}}"
ENV_B64_FILE="${1:-${ENV_B64_FILE:-}}"

echo "== 1. Base packages =="
export DEBIAN_FRONTEND=noninteractive
missing_packages=()
for bin in git curl nginx ffmpeg; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    missing_packages+=( "$bin" )
  fi
done

if [ "${#missing_packages[@]}" -gt 0 ]; then
  apt-get update
  apt-get install -y git curl nginx ffmpeg ca-certificates gnupg
else
  echo "Base packages already installed; skipping apt-get."
fi

node_major() {
  node -p "Number(process.versions.node.split('.')[0])" 2>/dev/null || echo 0
}

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1 || [ "$(node_major)" -lt 20 ]; then
  echo "Installing Node.js 20.x..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
  apt-get install -y nodejs
fi

echo "Node: $(node -v)"
echo "npm: $(npm -v)"
npm config set registry https://registry.npmmirror.com
if ! command -v pm2 >/dev/null 2>&1; then
  npm install -g pm2 --registry=https://registry.npmmirror.com
fi

echo "== 2. Source code =="
mkdir -p /var/www
if [ ! -d "$APP_DIR/.git" ]; then
  rm -rf "$APP_DIR"
  git clone "$REPO" "$APP_DIR"
fi
cd "$APP_DIR"
git fetch origin
git checkout master
git pull --ff-only origin master
if [ -f package-lock.json ]; then
  npm ci --omit=dev --registry=https://registry.npmmirror.com
else
  npm install --omit=dev --registry=https://registry.npmmirror.com
fi

echo "== 3. Environment =="
if [ -n "$ENV_B64_FILE" ] && [ -f "$ENV_B64_FILE" ]; then
  base64 -d "$ENV_B64_FILE" > .env
elif [ ! -f .env ]; then
  cp .env.example .env
fi

set_env() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" .env; then
    sed -i "s|^${key}=.*|${key}=${value}|g" .env
  else
    printf '\n%s=%s\n' "$key" "$value" >> .env
  fi
}

set_env NODE_ENV production
set_env PORT "$APP_PORT"
set_env PUBLIC_SITE_URL "$PUBLIC_URL"
set_env REDIRECT_FAST_BUDGET_MS 150
set_env REDIRECT_TIMEOUT_MS 4500
set_env PROVIDER_REQUEST_TIMEOUT_MS 4500
set_env MAX_CANDIDATE_URLS 2
set_env PARSE_PROVIDER_CONCURRENCY 6
set_env PARSE_CACHE_TTL_MINUTES 60
set_env AUTO_MEDIA_PROCESSING false
set_env ENABLE_VIDEO_OPTIMIZE false
set_env LOG_PROVIDER_TIMINGS true

echo "== 4. Validate code =="
node --check server.js

echo "== 5. Start app =="
PM2_HOME=/root/.pm2 pm2 delete clipflow 2>/dev/null || true
if [ -f ecosystem.config.cjs ]; then
  APP_DIR="$APP_DIR" PORT="$APP_PORT" PM2_HOME=/root/.pm2 pm2 start ecosystem.config.cjs --env production --update-env
else
  PORT="$APP_PORT" PM2_HOME=/root/.pm2 pm2 start server.js --name clipflow --update-env
fi
PM2_HOME=/root/.pm2 pm2 save
env PATH="$PATH" PM2_HOME=/root/.pm2 pm2 startup systemd -u root --hp /root || true

echo "== 6. Nginx =="
cat >/tmp/clipflow.nginx.conf <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name ${DOMAIN} www.${DOMAIN} 8.156.90.229;
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
        proxy_read_timeout 300;
        proxy_connect_timeout 60;
        proxy_send_timeout 300;
    }
}
EOF

if [ -d /etc/nginx/conf.d ]; then
  cp /tmp/clipflow.nginx.conf /etc/nginx/conf.d/clipflow.conf
  rm -f /etc/nginx/sites-enabled/clipflow
elif [ -d /etc/nginx/sites-available ] && [ -d /etc/nginx/sites-enabled ]; then
  cp /tmp/clipflow.nginx.conf /etc/nginx/sites-available/clipflow
  ln -sf /etc/nginx/sites-available/clipflow /etc/nginx/sites-enabled/clipflow
else
  echo "Cannot find an Nginx include directory under /etc/nginx." >&2
  exit 1
fi
rm -f /etc/nginx/sites-enabled/default /etc/nginx/conf.d/default.conf
nginx -t
systemctl enable nginx
systemctl reload nginx || systemctl restart nginx

echo "== 7. Verify =="
PM2_HOME=/root/.pm2 pm2 status
curl -fsS "http://127.0.0.1:${APP_PORT}/api/health" | head -c 1200
echo
curl -fsSI "http://127.0.0.1:${APP_PORT}/" | head
curl -fsSI "http://8.156.90.229/" | head || true
curl -fsSI "http://${DOMAIN}/" | head || true
echo "Done. HTTP 200 means the public site is up."
