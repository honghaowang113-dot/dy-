#!/usr/bin/env bash
set -euo pipefail

REPO="${1:-https://github.com/honghaowang113-dot/dy-.git}"
APP_DIR="${2:-/var/www/clipflow}"
DOMAIN="${3:-dyhonghao.top}"
APP_PORT="${APP_PORT:-${PORT:-3019}}"
PUBLIC_URL="${PUBLIC_URL:-http://${DOMAIN}}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export REPO APP_DIR DOMAIN APP_PORT PUBLIC_URL
exec bash "${SCRIPT_DIR}/run-mainland-deploy.sh"
