# ClipFlow 抖音内容提取工具

一个功能等价、品牌微调的抖音内容提取页面。前端是单页 `index.html`，后端是 Node Express，支持公开抖音链接解析、无水印视频下载、封面下载、BGM 下载、FFmpeg 生成 MP3、可选云转写和 ZIP 打包。

## 运行

```bash
npm.cmd install
npm.cmd start
```

打开 `http://localhost:3000`。

Windows PowerShell 默认可能禁止 `npm.ps1`，因此建议使用 `npm.cmd`。

如果你的 `.env` 里设置了 `PORT=3019`，本地测试地址就是 `http://localhost:3019`。

## 解析 Provider 配置

后端会按 `DOUYIN_PROVIDERS` 的顺序依次尝试 Provider：

```env
DOUYIN_PROVIDERS=tikhub,xinyew,mxin,jxcxin,devtool,makuo,mujie
```

当前内置 Provider：

- `xinyew`：免费接口，可能受可用性和证书状态影响。
- `mxin`：免费接口，适合做兜底测试。
- `jxcxin`：免费接口，当前稳定性一般。
- `devtool`：免费接口，当前稳定性一般。
- `tikhub`：需要 `TIKHUB_API_KEY`。端点来自 GitHub 项目 `TikHub/TikHub-Multi-Functional-Downloader`，优先调用 `/api/v1/douyin/app/v3/fetch_one_video_by_share_url`，再用 web 端点兜底。
- `makuo`：需要 `MAKUO_TOKEN`。
- `mujie`：需要 `MUJIE_KEY`。

缺少密钥的 Token Provider 会自动跳过。生产环境建议优先购买并配置一个稳定 Provider，然后把它放在 `DOUYIN_PROVIDERS` 第一位。

TikHub 示例：

```env
TIKHUB_BASE_URL=https://api.tikhub.io
TIKHUB_API_KEY=你的_TikHub_API_Key
DOUYIN_PROVIDERS=tikhub,xinyew,mxin,jxcxin,devtool
```

## 生产解析速度

如果线上更看重“快速返回解析结果”，建议关闭同步媒体后处理：

```env
AUTO_MEDIA_PROCESSING=false
ENABLE_VIDEO_OPTIMIZE=false
PROVIDER_REQUEST_TIMEOUT_MS=4500
TIKHUB_SECONDARY_ENDPOINT_DELAY_MS=600
REDIRECT_FAST_BUDGET_MS=150
PARSE_PROVIDER_CONCURRENCY=6
```

关闭后，页面会优先返回标题、作者、封面、无水印视频和 BGM 的下载入口；服务器不再为每个任务立即下载视频、转码 MP3 或调用转写接口。Provider 耗时会输出到 PM2 日志，便于排查慢接口。

## 转写配置

复制 `.env.example` 为 `.env` 后按需配置：

```env
TRANSCRIPTION_API_KEY=你的转写服务密钥
TRANSCRIPTION_BASE_URL=https://api.openai.com/v1/audio/transcriptions
TRANSCRIPTION_MODEL=whisper-1
TRANSCRIPTION_LANGUAGE=zh

# 可选：AI 文案改写，OpenAI 兼容 Chat Completions 接口。
# 不填 REWRITE_API_KEY 时，会尝试复用 TRANSCRIPTION_API_KEY。
REWRITE_API_KEY=
REWRITE_BASE_URL=https://api.siliconflow.cn/v1/chat/completions
REWRITE_MODEL=Qwen/Qwen2.5-7B-Instruct
```

不配置转写时，视频、封面、BGM、MP3 和 ZIP 下载仍可使用。

## API

- `GET /api/health`：查看 Provider、转写和运行限制配置状态。
- `GET /api/providers/test?url=抖音链接`：逐个测试 Provider，返回成功/失败原因，不执行下载和转码。
- `POST /api/extract`：提交 `{ "text": "抖音分享文案或链接" }` 或 `{ "urls": ["https://v.douyin.com/..."] }`。
- `GET /api/jobs/:id`：查询任务状态。
- `GET /api/download/:assetId`：下载单个文件，追加 `?inline=1` 可用于预览。
- `GET /api/jobs/:id/archive`：下载任务内全部可用文件的 ZIP。

## 手动支付二维码

当前支付模式是 `PAYMENT_PROVIDER=manual`，用户点击标准版或高级版的“立即开通”后，会创建一条待支付订单，并在页面里展示微信和支付宝二维码。

默认二维码文件：

- 微信：`assets/payments/wechat-qr.jpg`
- 支付宝：`assets/payments/alipay-qr.jpg`

把这两个文件替换为你自己的收款码图片即可。也可以在 `.env` 中配置外部图片地址：

```env
PAYMENT_PROVIDER=manual
PAYMENT_RECEIVER_NAME=你的收款名称
PAYMENT_WECHAT_QR_URL=/assets/payments/wechat-qr.jpg
PAYMENT_ALIPAY_QR_URL=/assets/payments/alipay-qr.jpg
PAYMENT_WEBHOOK_SECRET=替换为高强度随机字符串
```

要启用“支付成功自动升级”，请把支付平台回调地址配置为：

`https://你的域名/api/payments/webhook?token=PAYMENT_WEBHOOK_SECRET`

回调触发后，系统会自动把订单标记为已支付并升级对应套餐。

## 使用说明

第三方解析服务的稳定性和使用条款以服务方为准。请仅处理本人拥有授权或允许下载的公开内容。

## 商用 MVP

本地版本已经加入套餐、额度、账单记录、用户中心、管理员用户管理和合规入口。正式商用上线前，请阅读 [`COMMERCIAL_LAUNCH.md`](./COMMERCIAL_LAUNCH.md)，并至少完成：

当前会员规则：

- 游客体验期：2 次免注册，暂不支持批量。
- 免费版：每日 5 次、每月 150 次、单次最多 2 个链接。
- 标准版：19.9 元/月，每月 1500 次、无每日限制、单次最多 5 个链接。
- 高级版：99 元/月，每月 10000 次、单次最多 5 个链接、优先解析通道。

- 把 `data/users.json` 迁移到 PostgreSQL。
- 把长任务迁移到 Redis 队列。
- 把视频、封面、MP3、ZIP 迁移到对象存储。
- 接入微信支付、支付宝或 Stripe，并实现支付回调验签。
- 替换为明确允许商用的稳定解析 Provider。
- 修改 `.env` 中的管理员账号、`AUTH_SECRET`、域名和生产密钥。

## Public Deployment Quick Path

1. GitHub publish:
   - Create an empty repository on GitHub (no README).
   - Add remote and push:
```bash
git remote add origin https://github.com/<owner>/<repo>.git
git push -u origin master
```

2. Production environment:
   - Use `.env.example` as template.
   - Set `NODE_ENV=production`.
   - Set `AUTH_SECRET` to a strong random secret.
   - Set `PUBLIC_SITE_URL=https://www.yourdomain.com`.

3. Domain + HTTPS:
   - Buy domain from Cloudflare/Namecheap/Aliyun/Tencent Cloud.
   - Point `A` record to your server IP (or `CNAME` to your platform URL).
   - Enable HTTPS certificate (Let's Encrypt or platform managed SSL).

4. Search engine indexing:
   - Verify `https://www.yourdomain.com/robots.txt` is accessible.
   - Verify `https://www.yourdomain.com/sitemap.xml` is accessible.
   - Submit sitemap to Google Search Console and Bing Webmaster.

## Alipay Auto-Upgrade Setup (Production)

Use these environment variables to enable real Alipay payment + automatic plan upgrade:

```env
PAYMENT_PROVIDER=alipay
PAYMENT_WEBHOOK_SECRET=replace_with_long_random_secret
ALIPAY_APP_ID=your_app_id
ALIPAY_PRIVATE_KEY=your_rsa2_private_key_base64_or_pem
ALIPAY_PUBLIC_KEY=alipay_public_key_base64_or_pem
ALIPAY_GATEWAY=https://openapi.alipay.com/gateway.do
ALIPAY_NOTIFY_URL=https://your-domain/api/payments/webhook?token=PAYMENT_WEBHOOK_SECRET
```

After updating `.env`, restart:

```bash
env PM2_HOME=/root/.pm2 pm2 restart clipflow --update-env
```
