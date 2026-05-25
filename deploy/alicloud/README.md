# AliCloud ECS Deployment

Target:
- Domain: `dyhonghao.top`
- Server IP: `8.156.90.229`
- App port: `3019` (internal), Nginx exposes `80/443`.

## 1) Security group rules

In AliCloud ECS console, allow inbound:
- `22/tcp` from your admin IP
- `80/tcp` from `0.0.0.0/0`
- `443/tcp` from `0.0.0.0/0`

## 2) Login and deploy

```bash
ssh root@8.156.90.229
```

If you use another user:
```bash
ssh <user>@8.156.90.229
```

On a fresh server, run the deploy script from GitHub first. It installs missing system packages, ensures Node.js 20.x, clones/pulls the repo, installs Node dependencies, starts PM2, replaces the default Nginx site, and verifies `/api/health`.

```bash
curl -fsSL https://raw.githubusercontent.com/honghaowang113-dot/dy-/master/deploy/alicloud/run-mainland-deploy.sh \
  -o /tmp/clipflow-deploy.sh
bash /tmp/clipflow-deploy.sh
```

If `/var/www/clipflow` already exists, you can also run the repo-local wrapper:

```bash
bash /var/www/clipflow/deploy/alicloud/bootstrap.sh \
  https://github.com/honghaowang113-dot/dy-.git \
  /var/www/clipflow \
  dyhonghao.top
```

## 3) Enable HTTPS

This uses `acme.sh` with the existing Nginx webroot, so it does not install Certbot through apt.

```bash
bash /var/www/clipflow/deploy/alicloud/enable-https.sh dyhonghao.top your-email@example.com
```

## 4) Verify

```bash
curl -I http://dyhonghao.top
curl -s http://dyhonghao.top/api/health | head -c 1200; echo
curl -I https://dyhonghao.top
curl -I https://dyhonghao.top/robots.txt
curl -I https://dyhonghao.top/sitemap.xml
PM2_HOME=/root/.pm2 pm2 status
```

## 5) Troubleshooting

```bash
sudo nginx -t
sudo nginx -T | grep -E "clipflow|proxy_pass|server_name" -n
sudo systemctl status nginx
PM2_HOME=/root/.pm2 pm2 logs clipflow --lines 100
curl -s http://127.0.0.1:3019/api/health | head -c 1200; echo
```
