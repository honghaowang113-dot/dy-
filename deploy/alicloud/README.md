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

## 2) Login and bootstrap

```bash
ssh root@8.156.90.229
```

If you use another user:
```bash
ssh <user>@8.156.90.229
```

Then run:
```bash
bash /var/www/clipflow/deploy/alicloud/bootstrap.sh \
  https://github.com/honghaowang113-dot/dy-.git \
  /var/www/clipflow \
  dyhonghao.top
```

## 3) Enable HTTPS

```bash
bash /var/www/clipflow/deploy/alicloud/enable-https.sh dyhonghao.top your-email@example.com
```

## 4) Verify

```bash
curl -I http://dyhonghao.top
curl -I https://dyhonghao.top
curl -I https://dyhonghao.top/robots.txt
curl -I https://dyhonghao.top/sitemap.xml
pm2 status
```

## 5) Troubleshooting

```bash
sudo nginx -t
sudo systemctl status nginx
pm2 logs clipflow --lines 100
```
