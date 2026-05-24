#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:-/var/www/clipflow}"
cd "$APP_DIR"

python3 <<'PY'
from pathlib import Path

server = Path('server.js')
index = Path('index.html')
env_example = Path('.env.example')

text = server.read_text(encoding='utf-8')

if 'const PARSE_PROVIDER_CONCURRENCY =' not in text:
    text = text.replace(
        'const MAX_CANDIDATE_URLS = clamp(Number(process.env.MAX_CANDIDATE_URLS || 2), 1, 6);',
        'const MAX_CANDIDATE_URLS = clamp(Number(process.env.MAX_CANDIDATE_URLS || 2), 1, 6);\n'
        'const PARSE_PROVIDER_CONCURRENCY = clamp(Number(process.env.PARSE_PROVIDER_CONCURRENCY || 4), 1, 12);'
    )

if 'parseProviderConcurrency: PARSE_PROVIDER_CONCURRENCY' not in text:
    text = text.replace(
        '      redirectTimeoutMs: REDIRECT_TIMEOUT_MS,\n      redirectFastBudgetMs: REDIRECT_FAST_BUDGET_MS,',
        '      redirectTimeoutMs: REDIRECT_TIMEOUT_MS,\n'
        '      redirectFastBudgetMs: REDIRECT_FAST_BUDGET_MS,\n'
        '      parseProviderConcurrency: PARSE_PROVIDER_CONCURRENCY,'
    )

start = text.index('async function parseDouyinFast(url) {')
end = text.index('async function getCandidateUrlsFast(url) {')
new_block = r'''async function parseDouyinFast(url) {
  const cached = getParseResultFromCache(url);
  if (cached) return { ...cached, cached: true };

  const candidateUrls = await getCandidateUrlsFast(url);
  for (const candidateUrl of candidateUrls) {
    const candidateCached = getParseResultFromCache(candidateUrl);
    if (candidateCached) return { ...candidateCached, sourceUrl: url, cached: true };
  }

  const providers = getEnabledDouyinProviders();
  const failures = [];
  const startedAt = Date.now();
  const attempts = [];
  for (const provider of providers) {
    if (!provider.isConfigured()) {
      failures.push(`${provider.providerName}: 未配置，已跳过`);
      continue;
    }
    for (const candidateUrl of candidateUrls) attempts.push({ provider, candidateUrl });
  }

  try {
    const { parsed, candidateUrl } = await firstSuccessfulProviderAttempt(attempts, url, candidateUrls);
    const result = {
      ...parsed,
      sourceUrl: url,
      resolvedUrl: candidateUrl,
      cached: false,
      parseMs: Date.now() - startedAt
    };
    saveParseResultToCache([url, candidateUrl], result);
    return result;
  } catch (error) {
    failures.push(...(error.failures || []));
  }

  throw new Error(`解析服务不可用。${failures.slice(0, 8).join('；')}`);
}

function firstSuccessfulProviderAttempt(attempts, originalUrl, candidateUrls) {
  if (!attempts.length) {
    return Promise.reject(Object.assign(new Error('No configured parse providers.'), { failures: [] }));
  }

  return new Promise((resolve, reject) => {
    const failures = [];
    let cursor = 0;
    let inflight = 0;
    let settled = false;

    const launch = () => {
      if (settled) return;
      if (cursor >= attempts.length && inflight === 0) {
        const uniqueFailures = [...new Set(failures.filter(Boolean))];
        reject(Object.assign(new Error('All parse providers failed.'), { failures: uniqueFailures }));
        return;
      }

      while (!settled && inflight < PARSE_PROVIDER_CONCURRENCY && cursor < attempts.length) {
        const attempt = attempts[cursor];
        cursor += 1;
        inflight += 1;

        attempt.provider(attempt.candidateUrl, { originalUrl, candidateUrls })
          .then((parsed) => {
            if (settled) return;
            settled = true;
            resolve({ parsed, provider: attempt.provider, candidateUrl: attempt.candidateUrl });
          })
          .catch((error) => {
            failures.push(`${attempt.provider.providerName}: ${error.message || '解析失败'}`);
          })
          .finally(() => {
            inflight -= 1;
            launch();
          });
      }
    };

    launch();
  });
}

'''
text = text[:start] + new_block + text[end:]
server.write_text(text, encoding='utf-8')

html = index.read_text(encoding='utf-8')
html = html.replace(
    "if (item.status !== 'done' && item.status !== 'failed')",
    "if (item.status !== 'done' && item.status !== 'failed' && !item.parsed)"
)
index.write_text(html, encoding='utf-8')

if env_example.exists():
    env_text = env_example.read_text(encoding='utf-8')
    if 'PARSE_PROVIDER_CONCURRENCY=' not in env_text:
        env_text = env_text.replace(
            'MAX_CANDIDATE_URLS=2\n',
            'MAX_CANDIDATE_URLS=2\nPARSE_PROVIDER_CONCURRENCY=4\n'
        )
        env_example.write_text(env_text, encoding='utf-8')
PY

set_env() {
  local key="$1"
  local value="$2"
  if [ -f .env ] && grep -q "^${key}=" .env; then
    sed -i "s|^${key}=.*|${key}=${value}|g" .env
  else
    printf '\n%s=%s\n' "${key}" "${value}" >> .env
  fi
}

[ -f .env ] || cp .env.example .env
set_env REDIRECT_FAST_BUDGET_MS 1200
set_env REDIRECT_TIMEOUT_MS 4500
set_env PROVIDER_REQUEST_TIMEOUT_MS 7000
set_env PARSE_PROVIDER_CONCURRENCY 4
set_env MAX_CANDIDATE_URLS 2
set_env PARSE_CACHE_TTL_MINUTES 60

node --check server.js
PM2_HOME=/root/.pm2 pm2 restart clipflow --update-env || PM2_HOME=/root/.pm2 pm2 start server.js --name clipflow --update-env
PM2_HOME=/root/.pm2 pm2 save
curl -s http://127.0.0.1:3019/api/health | head -c 1200
echo

echo "Speed patch applied."
