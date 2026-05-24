import 'dotenv/config';

import { AlipaySdk } from 'alipay-sdk';
import archiver from 'archiver';
import express from 'express';
import QRCode from 'qrcode';
import { spawn } from 'node:child_process';
import { createHmac, randomBytes, randomInt, randomUUID, scrypt as scryptCallback, timingSafeEqual } from 'node:crypto';
import { createWriteStream, createReadStream } from 'node:fs';
import { mkdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const TMP_ROOT = join(__dirname, 'tmp', 'jobs');
const DATA_ROOT = join(__dirname, 'data');
const USERS_DB_PATH = join(DATA_ROOT, 'users.json');
const scryptAsync = promisify(scryptCallback);

const PORT = Number(process.env.PORT || 3000);
const SESSION_COOKIE = 'clipflow_session';
const GUEST_COOKIE = 'clipflow_guest';
const SESSION_TTL_MS = Number(process.env.SESSION_TTL_HOURS || 168) * 60 * 60 * 1000;
const GUEST_TTL_MS = Number(process.env.GUEST_TTL_DAYS || 365) * 24 * 60 * 60 * 1000;
const PUBLIC_SITE_URL = normalizePublicSiteUrl(process.env.PUBLIC_SITE_URL || process.env.PUBLIC_BASE_URL || `http://localhost:${PORT}`);
const AUTH_SECRET = process.env.AUTH_SECRET || randomUUID();
const DEFAULT_ADMIN_EMAIL = (process.env.DEFAULT_ADMIN_EMAIL || 'admin@clipflow.local').toLowerCase();
const DEFAULT_ADMIN_PASSWORD = process.env.DEFAULT_ADMIN_PASSWORD || 'Admin@123456';
const DEFAULT_ADMIN_NAME = process.env.DEFAULT_ADMIN_NAME || '管理员';
const USER_HISTORY_LIMIT = Number(process.env.USER_HISTORY_LIMIT || 50);
const MAX_LINKS_PER_JOB = Number(process.env.MAX_LINKS_PER_JOB || 5);
const COMMERCIAL_MODE = String(process.env.COMMERCIAL_MODE ?? 'true').toLowerCase() !== 'false';
const PAYMENT_PROVIDER = process.env.PAYMENT_PROVIDER || 'manual';
const PAYMENT_WEBHOOK_SECRET = process.env.PAYMENT_WEBHOOK_SECRET || '';
const PAYMENT_RECEIVER_NAME = process.env.PAYMENT_RECEIVER_NAME || 'ClipFlow';
const PAYMENT_WECHAT_QR_URL = process.env.PAYMENT_WECHAT_QR_URL || '/assets/payments/wechat-qr.jpg';
const PAYMENT_ALIPAY_QR_URL = process.env.PAYMENT_ALIPAY_QR_URL || '/assets/payments/alipay-qr.jpg';
const ALIPAY_APP_ID = String(process.env.ALIPAY_APP_ID || '').trim();
const ALIPAY_PRIVATE_KEY = String(process.env.ALIPAY_PRIVATE_KEY || '').trim();
const ALIPAY_PUBLIC_KEY = String(process.env.ALIPAY_PUBLIC_KEY || '').trim();
const ALIPAY_GATEWAY = String(process.env.ALIPAY_GATEWAY || 'https://openapi.alipay.com/gateway.do').trim();
const ALIPAY_NOTIFY_URL = String(
  process.env.ALIPAY_NOTIFY_URL
  || (PAYMENT_WEBHOOK_SECRET ? `${PUBLIC_SITE_URL}/api/payments/webhook?token=${encodeURIComponent(PAYMENT_WEBHOOK_SECRET)}` : '')
).trim();
const STORAGE_PROVIDER = process.env.STORAGE_PROVIDER || 'local';
const QUEUE_PROVIDER = process.env.QUEUE_PROVIDER || 'in-process';
const PLAN_CATALOG = Object.freeze({
  free: {
    id: 'free',
    name: '免费版',
    englishName: 'Free',
    description: '注册后可用，每日 5 次、每月最高 150 次',
    priceMonthly: 0,
    currency: 'CNY',
    dailyExtractLimit: 5,
    monthlyExtractLimit: 150,
    monthlyRewriteLimit: 30,
    monthlyTranscriptionMinutes: 10,
    maxLinksPerJob: Math.min(MAX_LINKS_PER_JOB, 2),
    priority: 0
  },
  standard: {
    id: 'standard',
    name: '标准版',
    englishName: 'Standard',
    description: '每月 1500 次解析，无每日限制，支持批量 5 个链接',
    priceMonthly: 19.9,
    currency: 'CNY',
    dailyExtractLimit: null,
    monthlyExtractLimit: 1500,
    monthlyRewriteLimit: 500,
    monthlyTranscriptionMinutes: 300,
    maxLinksPerJob: Math.min(MAX_LINKS_PER_JOB, 5),
    priority: 1
  },
  pro: {
    id: 'pro',
    name: '高级版',
    englishName: 'Pro',
    description: '每月 10000 次解析，优先解析通道，专属支持',
    priceMonthly: 99,
    currency: 'CNY',
    dailyExtractLimit: null,
    monthlyExtractLimit: 10000,
    monthlyRewriteLimit: 3000,
    monthlyTranscriptionMinutes: 1500,
    maxLinksPerJob: Math.min(MAX_LINKS_PER_JOB, 5),
    priority: 2
  }
});
const MAX_DOWNLOAD_BYTES = Number(process.env.MAX_DOWNLOAD_MB || 200) * 1024 * 1024;
const JOB_TTL_MS = Number(process.env.JOB_TTL_MINUTES || 120) * 60 * 1000;
const RATE_LIMIT_WINDOW_MS = 10 * 60 * 1000;
const RATE_LIMIT_MAX = 30;
const RATE_LIMIT_POLICIES = {
  extract: { name: 'extract', windowMs: RATE_LIMIT_WINDOW_MS, max: RATE_LIMIT_MAX },
  archive: { name: 'archive', windowMs: RATE_LIMIT_WINDOW_MS, max: 20 },
  status: { name: 'status', windowMs: 60 * 1000, max: 180 },
  download: { name: 'download', windowMs: 60 * 1000, max: 600 },
  auth: { name: 'auth', windowMs: RATE_LIMIT_WINDOW_MS, max: 120 },
  authCodeSend: { name: 'auth-code-send', windowMs: 60 * 1000, max: 20 },
  authCodeLogin: { name: 'auth-code-login', windowMs: 60 * 1000, max: 80 },
  rewrite: { name: 'rewrite', windowMs: RATE_LIMIT_WINDOW_MS, max: 60 },
  api: { name: 'api', windowMs: RATE_LIMIT_WINDOW_MS, max: RATE_LIMIT_MAX }
};
const ALLOW_WATERMARK_FALLBACK = String(process.env.ALLOW_WATERMARK_FALLBACK || '').toLowerCase() === 'true';
const ENABLE_VIDEO_OPTIMIZE = String(process.env.ENABLE_VIDEO_OPTIMIZE ?? 'true').toLowerCase() !== 'false';
const VIDEO_OPTIMIZE_CRF = clamp(Number(process.env.VIDEO_OPTIMIZE_CRF || 18), 14, 28);
const VIDEO_OPTIMIZE_PRESET = process.env.VIDEO_OPTIMIZE_PRESET || 'medium';
const PROVIDER_REQUEST_TIMEOUT_MS = clamp(Number(process.env.PROVIDER_REQUEST_TIMEOUT_MS || 9000), 3000, 30000);
const REDIRECT_TIMEOUT_MS = clamp(Number(process.env.REDIRECT_TIMEOUT_MS || 4500), 2000, 12000);
const REDIRECT_FAST_BUDGET_MS = clamp(Number(process.env.REDIRECT_FAST_BUDGET_MS || 1200), 500, 8000);
const REDIRECT_MAX_HOPS = clamp(Number(process.env.REDIRECT_MAX_HOPS || 3), 1, 5);
const MAX_CANDIDATE_URLS = clamp(Number(process.env.MAX_CANDIDATE_URLS || 2), 1, 6);
const PARSE_PROVIDER_CONCURRENCY = clamp(Number(process.env.PARSE_PROVIDER_CONCURRENCY || 4), 1, 12);
const PARSE_CACHE_TTL_MS = clamp(Number(process.env.PARSE_CACHE_TTL_MINUTES || 30), 1, 240) * 60 * 1000;
const DEFAULT_DOUYIN_PROVIDERS = ['tikhub', 'xinyew', 'mxin', 'jxcxin', 'devtool', 'makuo', 'mujie'];
const DOUYIN_PROVIDER_ORDER = parseProviderOrder(process.env.DOUYIN_PROVIDERS);

const allowedHostSuffixes = ['douyin.com', 'iesdouyin.com'];
const jobs = new Map();
const assets = new Map();
const parseCache = new Map();
const rateWindows = new Map();
const smsCodes = new Map();
let alipayClient = null;

await mkdir(TMP_ROOT, { recursive: true });
await mkdir(DATA_ROOT, { recursive: true });
await ensureUserStore();
await ensureDefaultAdminAccount();

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json({ limit: '128kb' }));
app.use('/api', rateLimit);
app.use(express.static(__dirname));

app.get('/robots.txt', (_req, res) => {
  res.type('text/plain').send(
    [
      'User-agent: *',
      'Allow: /',
      '',
      `Sitemap: ${PUBLIC_SITE_URL}/sitemap.xml`
    ].join('\n')
  );
});

app.get('/sitemap.xml', (_req, res) => {
  const now = new Date().toISOString();
  const urls = ['/', '/#extract', '/#plans', '/#faq'];
  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...urls.map((path) => {
      const loc = `${PUBLIC_SITE_URL}${path}`;
      return `<url><loc>${escapeXml(loc)}</loc><lastmod>${now}</lastmod><changefreq>daily</changefreq><priority>0.8</priority></url>`;
    }),
    '</urlset>'
  ].join('');
  res.type('application/xml').send(xml);
});

app.post('/api/auth/register', async (req, res) => {
  try {
    const { identifier, type } = normalizeAccountIdentifier(req.body?.identifier);
    const password = String(req.body?.password || '');
    const displayName = safeDisplayName(req.body?.displayName, identifier);
    if (!isStrongPassword(password)) {
      return res.status(400).json({ error: '密码至少 8 位，并同时包含字母和数字。' });
    }

    const db = await readUserDb();
    if (findUserByIdentifier(db.users, identifier)) {
      return res.status(409).json({ error: '账号已存在，请直接登录。' });
    }

    const adminExists = db.users.some((user) => user.role === 'admin');
    const now = new Date().toISOString();
    const user = {
      id: randomUUID(),
      accountType: type,
      email: type === 'email' ? identifier : '',
      phone: type === 'phone' ? identifier : '',
      displayName,
      role: adminExists ? 'user' : 'admin',
      status: 'active',
      planId: adminExists ? 'free' : 'pro',
      quotaOverrides: {},
      billingRecords: [],
      passwordHash: await hashPassword(password),
      createdAt: now,
      updatedAt: now,
      lastLoginAt: now,
      usageStats: createEmptyUsageStats(),
      history: []
    };
    db.users.push(user);
    await writeUserDb(db);
    setSessionCookie(res, user.id);
    res.status(201).json({ user: publicUser(user) });
  } catch (error) {
    res.status(400).json({ error: error.message || '注册失败。' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { identifier } = normalizeAccountIdentifier(req.body?.identifier);
    const password = String(req.body?.password || '');
    const db = await readUserDb();
    const user = findUserByIdentifier(db.users, identifier);
    if (!user || !(await verifyPassword(password, user.passwordHash))) {
      return res.status(401).json({ error: '账号或密码错误。' });
    }
    if (user.status !== 'active') {
      return res.status(403).json({ error: '账号已被禁用，请联系管理员。' });
    }

    user.lastLoginAt = new Date().toISOString();
    user.updatedAt = user.updatedAt || user.lastLoginAt;
    await writeUserDb(db);
    setSessionCookie(res, user.id);
    res.json({ user: publicUser(user) });
  } catch (error) {
    res.status(400).json({ error: error.message || '登录失败。' });
  }
});

app.post('/api/auth/code/send', async (req, res) => {
  try {
    const { identifier: phone } = normalizePhoneLogin(req.body);
    const code = String(randomInt(100000, 999999));
    smsCodes.set(phone, {
      code,
      expiresAt: Date.now() + 5 * 60 * 1000,
      attempts: 0
    });

    res.json({
      ok: true,
      expiresInSeconds: 300,
      debugCode: process.env.NODE_ENV === 'production' ? undefined : code
    });
  } catch (error) {
    res.status(400).json({ error: error.message || '验证码发送失败。' });
  }
});

app.post('/api/auth/code/login', async (req, res) => {
  try {
    const { identifier: phone } = normalizePhoneLogin(req.body);
    const code = String(req.body?.code || '').trim();
    if (!/^\d{6}$/.test(code)) {
      return res.status(400).json({ error: '请输入 6 位验证码。' });
    }

    const stored = smsCodes.get(phone);
    if (!stored || stored.expiresAt < Date.now()) {
      smsCodes.delete(phone);
      return res.status(400).json({ error: '验证码已过期，请重新获取。' });
    }
    stored.attempts += 1;
    if (stored.attempts > 5 || stored.code !== code) {
      return res.status(400).json({ error: '验证码不正确。' });
    }
    smsCodes.delete(phone);

    const db = await readUserDb();
    let user = findUserByIdentifier(db.users, phone);
    const now = new Date().toISOString();
    if (!user) {
      const adminExists = db.users.some((item) => item.role === 'admin');
      user = {
        id: randomUUID(),
        accountType: 'phone',
        email: '',
        phone,
        displayName: safeDisplayName(req.body?.displayName, phone),
        role: adminExists ? 'user' : 'admin',
        status: 'active',
        planId: adminExists ? 'free' : 'pro',
        quotaOverrides: {},
        billingRecords: [],
        passwordHash: '',
        createdAt: now,
        updatedAt: now,
        lastLoginAt: now,
        usageStats: createEmptyUsageStats(),
        history: []
      };
      db.users.push(user);
    }

    if (user.status !== 'active') {
      return res.status(403).json({ error: '账号已被禁用，请联系管理员。' });
    }

    user.lastLoginAt = now;
    user.updatedAt = now;
    await writeUserDb(db);
    setSessionCookie(res, user.id);
    res.json({ user: publicUser(user) });
  } catch (error) {
    res.status(400).json({ error: error.message || '验证码登录失败。' });
  }
});

app.post('/api/auth/logout', (_req, res) => {
  clearSessionCookie(res);
  res.json({ ok: true });
});

app.get('/api/auth/me', async (req, res) => {
  const user = await getRequestUser(req);
  res.json({
    authenticated: Boolean(user),
    user: user ? publicUser(user) : null,
    guestQuota: user ? null : guestQuotaView(req)
  });
});

app.get('/api/users/me/usage', async (req, res) => {
  const user = await getRequestUser(req);
  if (!user) {
    return res.status(401).json({ error: '请先登录。' });
  }
  res.json(userUsageView(user));
});

app.get('/api/users/me/history/:historyId', async (req, res) => {
  const user = await getRequestUser(req);
  if (!user) {
    return res.status(401).json({ error: '请先登录。' });
  }
  const entry = normalizeUsageHistory(user.history).find((item) => item.id === req.params.historyId);
  if (!entry) {
    return res.status(404).json({ error: '历史任务不存在。' });
  }
  const liveJob = entry.jobId ? jobs.get(entry.jobId) : null;
  if (liveJob && liveJob.userId && liveJob.userId !== user.id) {
    return res.status(403).json({ error: '无权访问该任务。' });
  }
  res.json(historyDetailView(entry, liveJob));
});

app.get('/api/plans', (_req, res) => {
  res.json({
    commercialMode: COMMERCIAL_MODE,
    paymentProvider: PAYMENT_PROVIDER,
    guest: {
      id: 'guest',
      name: '游客体验期',
      totalExtractLimit: 2,
      maxLinksPerJob: 1,
      requiresRegistration: false
    },
    plans: planCatalogView()
  });
});

app.get('/api/billing/me', async (req, res) => {
  const user = await getRequestUser(req);
  if (!user) {
    return res.status(401).json({ error: '请先登录。' });
  }
  res.json({
    user: publicUser(user),
    quota: quotaView(user),
    billingRecords: normalizeBillingRecords(user.billingRecords)
  });
});

app.post('/api/billing/checkout', async (req, res) => {
  const user = await getRequestUser(req);
  if (!user) {
    return res.status(401).json({ error: '请先登录。' });
  }

  const planId = normalizePlanId(req.body?.planId);
  if (planId === 'free') {
    return res.status(400).json({ error: '免费版不需要创建支付订单。' });
  }
  const plan = PLAN_CATALOG[planId];
  const db = await readUserDb();
  const target = db.users.find((item) => item.id === user.id);
  if (!target) {
    return res.status(404).json({ error: '账号不存在。' });
  }
  const now = new Date().toISOString();
  const paymentId = randomUUID();
  const orderNote = `ClipFlow-${paymentId.slice(0, 8).toUpperCase()}`;
  const payment = {
    id: paymentId,
    planId,
    planName: plan.name,
    amount: plan.priceMonthly,
    currency: plan.currency,
    status: 'pending_payment',
    provider: PAYMENT_PROVIDER,
    channel: '',
    tradeNo: '',
    orderNote,
    createdAt: now,
    paidAt: '',
    updatedAt: now
  };
  target.billingRecords = normalizeBillingRecords(target.billingRecords);
  target.billingRecords.unshift(payment);
  target.updatedAt = now;
  const provider = PAYMENT_PROVIDER === 'alipay' ? 'alipay' : 'manual';
  payment.provider = provider;
  target.billingRecords[0] = payment;

  let checkoutUrl = null;
  let manualPayment = null;

  if (provider === 'alipay') {
    if (!isAlipayConfigured()) {
      return res.status(503).json({ error: '支付宝支付未配置完成，请先配置 ALIPAY_APP_ID / ALIPAY_PRIVATE_KEY / ALIPAY_PUBLIC_KEY / PAYMENT_WEBHOOK_SECRET。' });
    }
    try {
      const precreate = await createAlipayPrecreateOrder({ payment, plan, user: target });
      checkoutUrl = precreate.qrCode || null;
      manualPayment = alipayCheckoutView(payment, plan, precreate);
      payment.channel = 'alipay';
      payment.updatedAt = new Date().toISOString();
      target.billingRecords[0] = payment;
    } catch (error) {
      return res.status(502).json({ error: `支付宝下单失败：${error.message || '未知错误'}` });
    }
  } else {
    manualPayment = manualPaymentView(payment, plan);
  }

  await writeUserDb(db);
  const autoUpgradeEnabled = Boolean(PAYMENT_WEBHOOK_SECRET);
  res.status(201).json({
    payment,
    checkoutUrl,
    manualPayment,
    autoUpgrade: {
      enabled: autoUpgradeEnabled,
      statusUrl: `/api/billing/orders/${payment.id}`,
      pollIntervalMs: 4000
    },
    message: autoUpgradeEnabled
      ? '订单已创建，支付成功后将自动升级套餐。'
      : '订单已创建。当前未配置支付回调，暂时无法自动升级。'
  });
});

app.get('/api/billing/orders/:paymentId', async (req, res) => {
  const user = await getRequestUser(req);
  if (!user) {
    return res.status(401).json({ error: '请先登录。' });
  }
  const payment = normalizeBillingRecords(user.billingRecords).find((item) => item.id === req.params.paymentId);
  if (!payment) {
    return res.status(404).json({ error: '订单不存在。' });
  }
  const activePlanId = normalizePlanId(user.planId);
  res.json({
    payment,
    isPaid: isPaidPaymentStatus(payment.status),
    activePlanId,
    activePlanName: PLAN_CATALOG[activePlanId].name
  });
});

app.post('/api/payments/webhook', async (req, res) => {
  if (!isPaymentWebhookAuthorized(req)) {
    return res.status(401).type('text/plain').send('unauthorized');
  }
  if (isLikelyAlipayNotify(req) && !verifyAlipayNotifySignature(req)) {
    return res.status(401).type('text/plain').send('invalid alipay sign');
  }
  const payload = normalizeWebhookPayload(req);
  if (!payload.orderId) {
    return res.status(400).type('text/plain').send('missing order id');
  }
  if (!payload.isPaid) {
    return res.status(202).type('text/plain').send('ignored');
  }
  const marked = await markOrderPaidAndUpgrade(payload);
  if (!marked.ok) {
    return res.status(404).type('text/plain').send('order not found');
  }
  return res.status(200).type('text/plain').send('success');
});

app.get('/api/admin/users', requireAdmin, async (req, res) => {
  const db = await readUserDb();
  const keyword = String(req.query.q || '').trim().toLowerCase();
  const users = keyword
    ? db.users.filter((user) => {
      const text = [user.email, user.phone, user.displayName, user.role, user.status, user.planId].join(' ').toLowerCase();
      return text.includes(keyword);
    })
    : db.users;
  res.json({ users: users.map(adminUserView), plans: planCatalogView() });
});

app.patch('/api/admin/users/:id', requireAdmin, async (req, res) => {
  const db = await readUserDb();
  const target = db.users.find((user) => user.id === req.params.id);
  if (!target) {
    return res.status(404).json({ error: '用户不存在。' });
  }

  if (req.body?.role === 'admin' && target.role !== 'admin') {
    return res.status(400).json({ error: '系统只允许一个管理员账号，普通用户不能升级为管理员。' });
  }
  if (req.body?.role === 'user' && target.role === 'admin') {
    return res.status(400).json({ error: '管理员账号不能降级。' });
  }

  const nextRole = target.role === 'admin' ? 'admin' : 'user';
  const nextStatus = req.body?.status === 'disabled' ? 'disabled' : req.body?.status === 'active' ? 'active' : target.status;
  if (target.role === 'admin' && nextStatus !== 'active') {
    return res.status(400).json({ error: '管理员账号不能禁用。' });
  }

  if (typeof req.body?.displayName === 'string') {
    target.displayName = safeDisplayName(req.body.displayName, target.email || target.phone);
  }
  if (target.role !== 'admin' && typeof req.body?.planId === 'string') {
    target.planId = normalizePlanId(req.body.planId);
  }
  if (target.role !== 'admin' && req.body?.quotaOverrides && typeof req.body.quotaOverrides === 'object') {
    target.quotaOverrides = normalizeQuotaOverrides(req.body.quotaOverrides);
  }
  target.role = nextRole;
  target.status = nextStatus;
  target.updatedAt = new Date().toISOString();
  await writeUserDb(db);
  res.json({ user: adminUserView(target) });
});

app.delete('/api/admin/users/:id', requireAdmin, async (req, res) => {
  const db = await readUserDb();
  const index = db.users.findIndex((user) => user.id === req.params.id);
  if (index < 0) {
    return res.status(404).json({ error: '用户不存在。' });
  }
  const target = db.users[index];
  if (target.role === 'admin') {
    return res.status(400).json({ error: '管理员账号不能删除。' });
  }
  if (target.id === req.user.id) {
    return res.status(400).json({ error: '不能删除当前登录账号。' });
  }
  db.users.splice(index, 1);
  await writeUserDb(db);
  res.json({ ok: true, deletedUserId: target.id });
});

app.post('/api/extract', async (req, res) => {
  try {
    const urls = normalizeRequestUrls(req.body);
    if (urls.length === 0) {
      return res.status(400).json({ error: '请粘贴至少一个抖音链接。' });
    }
    const user = await getRequestUser(req);
    const guestQuota = user ? null : guestQuotaView(req);
    const planLimit = user ? effectivePlanForUser(user).maxLinksPerJob : guestQuota.maxLinksPerJob;
    if (urls.length > planLimit) {
      const message = user
        ? batchLimitMessage(user, planLimit)
        : '游客体验期暂不支持批量解析。注册免费版可批量解析 2 个链接，升级标准版可解锁最多 5 个链接。';
      return res.status(402).json({ error: message, reason: 'batch_limit', guestQuota });
    }
    if (!user && guestQuota.remaining < 1) {
      return res.status(402).json({
        error: '您的免费体验额度已用完，注册账号即可继续使用免费版权益：每日 5 次解析、每月最高 150 次、批量处理 2 个链接。',
        reason: 'guest_quota_exhausted',
        guestQuota
      });
    }
    let quotaAfter = null;
    if (user) {
      quotaAfter = await consumeUserQuota(user.id, 'extract', urls.length);
    }
    const job = createJob(urls, user?.id);
    if (user && quotaAfter) {
      job.quotaNotice = buildExtractQuotaNotice(user, quotaAfter);
      job.quota = quotaAfter;
    }
    if (!user) {
      const nextGuestQuota = consumeGuestQuota(res, guestQuota.used + 1);
      job.guestQuota = nextGuestQuota;
      job.quotaNotice = buildGuestQuotaNotice(nextGuestQuota);
    }
    jobs.set(job.id, job);
    if (user) {
      await recordJobSubmitted(user.id, job, urls);
    }
    void processJob(job);

    res.status(202).json(toPublicJob(job));
  } catch (error) {
    res.status(error.statusCode || 400).json({ error: error.message || '提交失败。' });
  }
});

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) {
    return res.status(404).json({ error: '任务不存在或已过期。' });
  }
  res.json(toPublicJob(job));
});

app.post('/api/rewrite', async (req, res) => {
  try {
    const user = await getRequestUser(req);
    if (COMMERCIAL_MODE && !user) {
      return res.status(401).json({ error: '请先登录后再使用 AI 改写。' });
    }
    const sourceText = String(req.body?.text || '').trim();
    const language = String(req.body?.language || 'zh').toLowerCase() === 'en' ? 'en' : 'zh';
    if (sourceText.length < 4) {
      return res.status(400).json({ error: language === 'en' ? 'Not enough copy to rewrite.' : '可改写的文案太短。' });
    }
    if (sourceText.length > 3000) {
      return res.status(400).json({ error: language === 'en' ? 'Copy is too long. Keep it under 3000 characters.' : '文案过长，请控制在 3000 字以内。' });
    }

    if (user) {
      await consumeUserQuota(user.id, 'rewrite', 1);
    }
    const result = await rewriteCopy({ sourceText, language });
    res.json(result);
  } catch (error) {
    res.status(error.statusCode || 500).json({ error: error.message || 'AI 改写失败。' });
  }
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    providers: getProviderStatus(),
    transcription: {
      configured: Boolean(process.env.TRANSCRIPTION_API_KEY),
      model: process.env.TRANSCRIPTION_MODEL || 'whisper-1',
      baseUrl: process.env.TRANSCRIPTION_BASE_URL || 'https://api.openai.com/v1/audio/transcriptions'
    },
    rewrite: {
      configured: Boolean(process.env.REWRITE_API_KEY || process.env.TRANSCRIPTION_API_KEY),
      model: process.env.REWRITE_MODEL || 'Qwen/Qwen2.5-7B-Instruct',
      baseUrl: process.env.REWRITE_BASE_URL || 'https://api.siliconflow.cn/v1/chat/completions'
    },
    limits: {
      maxLinksPerJob: MAX_LINKS_PER_JOB,
      maxDownloadMb: Number(process.env.MAX_DOWNLOAD_MB || 200),
      jobTtlMinutes: Number(process.env.JOB_TTL_MINUTES || 120),
      providerTimeoutMs: PROVIDER_REQUEST_TIMEOUT_MS,
      redirectTimeoutMs: REDIRECT_TIMEOUT_MS,
      redirectFastBudgetMs: REDIRECT_FAST_BUDGET_MS,
      parseProviderConcurrency: PARSE_PROVIDER_CONCURRENCY,
      parseCacheTtlMinutes: Math.round(PARSE_CACHE_TTL_MS / 60000)
    },
    commercial: {
      enabled: COMMERCIAL_MODE,
      paymentProvider: PAYMENT_PROVIDER,
      paymentAutoUpgrade: Boolean(PAYMENT_WEBHOOK_SECRET),
      alipayConfigured: isAlipayConfigured(),
      storageProvider: STORAGE_PROVIDER,
      queueProvider: QUEUE_PROVIDER,
      database: process.env.DATABASE_URL ? 'postgres-configured' : 'local-json',
      redis: process.env.REDIS_URL ? 'configured' : 'not-configured',
      objectStorage: STORAGE_PROVIDER === 'local' ? 'local-temp-files' : STORAGE_PROVIDER
    }
  });
});

app.get('/api/providers/test', async (req, res) => {
  try {
    const targetUrl = validateDouyinUrl(String(req.query.url || ''));
    const candidateUrls = await getCandidateUrls(targetUrl);
    const results = [];

    for (const provider of getEnabledProviders()) {
      if (!provider.isConfigured()) {
        results.push({
          provider: provider.providerName,
          configured: false,
          status: 'skipped',
          message: '未配置，已跳过'
        });
        continue;
      }

      const startedAt = Date.now();
      try {
        const parsed = await provider(candidateUrls[0], { originalUrl: targetUrl, candidateUrls });
        results.push({
          provider: provider.providerName,
          configured: true,
          status: 'ok',
          elapsedMs: Date.now() - startedAt,
          title: parsed.title,
          hasVideo: Boolean(parsed.videoUrl),
          hasCover: Boolean(parsed.coverUrl),
          hasMusic: Boolean(parsed.musicUrl)
        });
      } catch (error) {
        results.push({
          provider: provider.providerName,
          configured: true,
          status: 'failed',
          elapsedMs: Date.now() - startedAt,
          message: error.message
        });
      }
    }

    res.json({ url: targetUrl, candidateUrls, results });
  } catch (error) {
    res.status(400).json({ error: error.message || 'Provider 测试失败。' });
  }
});

app.get('/api/download/:assetId', async (req, res) => {
  const asset = assets.get(req.params.assetId);
  if (!asset) {
    return res.status(404).json({ error: '文件不存在或已过期。' });
  }

  try {
    if (asset.localPath) {
      return sendLocalAsset(req, res, asset);
    }

    if (!asset.remoteUrl) {
      return res.status(404).json({ error: '文件还没有生成。' });
    }

    const upstream = await fetchWithTimeout(asset.remoteUrl, {
      headers: requestHeaders()
    }, 60000);
    if (!upstream.ok || !upstream.body) {
      throw new Error(`远程文件下载失败：HTTP ${upstream.status}`);
    }

    res.setHeader('Content-Type', upstream.headers.get('content-type') || asset.contentType || 'application/octet-stream');
    res.setHeader('Content-Disposition', contentDisposition(asset.filename, req.query.inline === '1'));
    const length = upstream.headers.get('content-length');
    if (length) {
      res.setHeader('Content-Length', length);
    }
    await pipeResponse(Readable.fromWeb(upstream.body), res);
  } catch (error) {
    if (res.headersSent) {
      res.destroy(error);
      return;
    }
    res.status(502).json({ error: error.message || '下载失败。' });
  }
});

app.get('/api/jobs/:id/archive', async (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) {
    return res.status(404).json({ error: '任务不存在或已过期。' });
  }

  res.setHeader('Content-Type', 'application/zip');
  res.setHeader('Content-Disposition', contentDisposition(`douyin-assets-${job.id}.zip`));
  res.setHeader('Cache-Control', 'no-store');

  const archive = archiver('zip', { zlib: { level: 8 } });
  archive.on('error', (error) => {
    if (!res.headersSent) {
      res.status(500).json({ error: error.message });
    } else {
      res.destroy(error);
    }
  });
  archive.pipe(res);

  const errors = [];
  let filesAdded = 0;
  for (const [itemIndex, item] of job.items.entries()) {
    if (!item.assets) continue;
    const folder = archiveFolderName(item, itemIndex);
    let assetIndex = 0;
    for (const assetId of Object.values(item.assets)) {
      const asset = assets.get(assetId);
      if (!asset) continue;
      try {
        await ensureAssetLocal(asset, job.id, item.id);
        assetIndex += 1;
        archive.file(asset.localPath, { name: `${folder}/${archiveAssetName(asset, itemIndex, assetIndex)}` });
        filesAdded += 1;
      } catch (error) {
        errors.push(`${asset.filename}: ${error.message}`);
      }
    }
  }

  archive.append(JSON.stringify({
    jobId: job.id,
    status: job.status,
    itemCount: job.items.length,
    fileCount: filesAdded,
    exportedAt: new Date().toISOString()
  }, null, 2), { name: 'manifest.json' });

  if (errors.length) {
    archive.append(errors.join('\n'), { name: 'download-errors.txt' });
  }

  await archive.finalize();
});

app.use((_req, res) => {
  res.sendFile(join(__dirname, 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Douyin extractor running at http://localhost:${PORT}`);
});

setInterval(cleanExpiredJobs, 15 * 60 * 1000).unref();
setInterval(cleanExpiredSmsCodes, 60 * 1000).unref();
setInterval(cleanExpiredParseCache, 5 * 60 * 1000).unref();

function rateLimit(req, res, next) {
  const policy = getRateLimitPolicy(req);
  if (!policy) {
    return next();
  }

  const key = `${policy.name}:${req.ip || req.socket.remoteAddress || 'anonymous'}`;
  const now = Date.now();
  const window = rateWindows.get(key) || { start: now, count: 0 };
  if (now - window.start > policy.windowMs) {
    window.start = now;
    window.count = 0;
  }
  window.count += 1;
  rateWindows.set(key, window);
  if (window.count > policy.max) {
    const retryAfter = Math.max(1, Math.ceil((window.start + policy.windowMs - now) / 1000));
    res.setHeader('Retry-After', String(retryAfter));
    return res.status(429).json({ error: '请求过于频繁，请稍后再试。' });
  }
  next();
}

function getRateLimitPolicy(req) {
  const path = req.path || '';
  if (req.method === 'GET' && path === '/health') {
    return null;
  }
  if (path === '/payments/webhook') {
    return null;
  }
  if (req.method === 'GET' && /^\/jobs\/[^/]+$/.test(path)) {
    return RATE_LIMIT_POLICIES.status;
  }
  if (req.method === 'GET' && /^\/billing\/orders\/[^/]+$/.test(path)) {
    return RATE_LIMIT_POLICIES.status;
  }
  if (req.method === 'GET' && /^\/download\/[^/]+$/.test(path)) {
    return RATE_LIMIT_POLICIES.download;
  }
  if (req.method === 'GET' && /^\/jobs\/[^/]+\/archive$/.test(path)) {
    return RATE_LIMIT_POLICIES.archive;
  }
  if (req.method === 'POST' && path === '/extract') {
    return RATE_LIMIT_POLICIES.extract;
  }
  if (req.method === 'POST' && path === '/rewrite') {
    return RATE_LIMIT_POLICIES.rewrite;
  }
  if (req.method === 'POST' && path === '/auth/code/send') {
    return RATE_LIMIT_POLICIES.authCodeSend;
  }
  if (req.method === 'POST' && path === '/auth/code/login') {
    return RATE_LIMIT_POLICIES.authCodeLogin;
  }
  if (path.startsWith('/auth/')) {
    return RATE_LIMIT_POLICIES.auth;
  }
  return RATE_LIMIT_POLICIES.api;
}

async function requireAdmin(req, res, next) {
  const user = await getRequestUser(req);
  if (!user) {
    return res.status(401).json({ error: '请先登录。' });
  }
  if (user.role !== 'admin' || user.status !== 'active') {
    return res.status(403).json({ error: '需要管理员权限。' });
  }
  req.user = user;
  next();
}

async function ensureUserStore() {
  try {
    await readFile(USERS_DB_PATH, 'utf8');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    await writeUserDb({ users: [] });
  }
}

async function ensureDefaultAdminAccount() {
  if (!DEFAULT_ADMIN_EMAIL || !DEFAULT_ADMIN_PASSWORD) return;
  const db = await readUserDb();
  const now = new Date().toISOString();
  let admin = findUserByIdentifier(db.users, DEFAULT_ADMIN_EMAIL);

  if (!admin) {
    admin = {
      id: randomUUID(),
      accountType: 'email',
      email: DEFAULT_ADMIN_EMAIL,
      phone: '',
      displayName: DEFAULT_ADMIN_NAME,
      role: 'admin',
      status: 'active',
      passwordHash: '',
      createdAt: now,
      updatedAt: now,
      lastLoginAt: '',
      usageStats: createEmptyUsageStats(),
      history: []
    };
    db.users.unshift(admin);
  }

  for (const user of db.users) {
    if (user.id !== admin.id && user.role === 'admin') {
      user.role = 'user';
      user.updatedAt = now;
    }
  }

  admin.accountType = 'email';
  admin.email = DEFAULT_ADMIN_EMAIL;
  admin.displayName = DEFAULT_ADMIN_NAME;
  admin.role = 'admin';
  admin.status = 'active';
  admin.planId = 'pro';
  admin.quotaOverrides = normalizeQuotaOverrides(admin.quotaOverrides);
  admin.billingRecords = normalizeBillingRecords(admin.billingRecords);
  admin.passwordHash = await hashPassword(DEFAULT_ADMIN_PASSWORD);
  admin.updatedAt = now;
  admin.usageStats = normalizeUsageStats(admin.usageStats);
  admin.history = normalizeUsageHistory(admin.history);
  await writeUserDb(db);
}

async function readUserDb() {
  try {
    const raw = await readFile(USERS_DB_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed.users)) return { users: [] };
    return { users: normalizeUserCollection(parsed.users) };
  } catch (error) {
    if (error.code === 'ENOENT') return { users: [] };
    throw error;
  }
}

async function writeUserDb(db) {
  await mkdir(DATA_ROOT, { recursive: true });
  await writeFile(USERS_DB_PATH, `${JSON.stringify({ users: normalizeUserCollection(db.users || []) }, null, 2)}\n`, 'utf8');
}

function normalizeUserCollection(users = []) {
  let adminSeen = false;
  return users.map((user) => {
    const normalized = normalizeUserRecord(user);
    if (normalized.role === 'admin') {
      if (adminSeen) {
        normalized.role = 'user';
      } else {
        adminSeen = true;
        normalized.status = 'active';
      }
    }
    return normalized;
  });
}

function normalizeUserRecord(user = {}) {
  const planId = normalizePlanId(user.planId || (user.role === 'admin' ? 'pro' : 'free'));
  return {
    ...user,
    role: user.role === 'admin' ? 'admin' : 'user',
    status: user.status === 'disabled' ? 'disabled' : 'active',
    planId,
    quotaOverrides: normalizeQuotaOverrides(user.quotaOverrides),
    billingRecords: normalizeBillingRecords(user.billingRecords),
    usageStats: normalizeUsageStats(user.usageStats),
    history: normalizeUsageHistory(user.history)
  };
}

function createEmptyUsageStats() {
  return {
    totalJobs: 0,
    totalLinks: 0,
    successItems: 0,
    failedItems: 0,
    usagePeriod: currentMonthKey(),
    usageDay: currentDayKey(),
    monthlyExtracts: 0,
    dailyExtracts: 0,
    monthlyRewrites: 0,
    monthlyTranscriptionSeconds: 0,
    lastUsedAt: ''
  };
}

function normalizeUsageStats(stats = {}) {
  const normalized = {
    totalJobs: Number(stats.totalJobs || 0),
    totalLinks: Number(stats.totalLinks || 0),
    successItems: Number(stats.successItems || 0),
    failedItems: Number(stats.failedItems || 0),
    usagePeriod: String(stats.usagePeriod || currentMonthKey()),
    usageDay: String(stats.usageDay || currentDayKey()),
    monthlyExtracts: Number(stats.monthlyExtracts || 0),
    dailyExtracts: Number(stats.dailyExtracts || 0),
    monthlyRewrites: Number(stats.monthlyRewrites || 0),
    monthlyTranscriptionSeconds: Number(stats.monthlyTranscriptionSeconds || 0),
    lastUsedAt: String(stats.lastUsedAt || '')
  };
  return resetUsageWindows(normalized);
}

function currentMonthKey(date = new Date()) {
  return date.toISOString().slice(0, 7);
}

function currentDayKey(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function resetUsageWindows(stats) {
  const month = currentMonthKey();
  const day = currentDayKey();
  if (stats.usagePeriod !== month) {
    stats.usagePeriod = month;
    stats.monthlyExtracts = 0;
    stats.monthlyRewrites = 0;
    stats.monthlyTranscriptionSeconds = 0;
  }
  if (stats.usageDay !== day) {
    stats.usageDay = day;
    stats.dailyExtracts = 0;
  }
  return stats;
}

function normalizePlanId(value) {
  return Object.hasOwn(PLAN_CATALOG, value) ? value : 'free';
}

function normalizeQuotaOverrides(value = {}) {
  if (!value || typeof value !== 'object') return {};
  const allowed = [
    'dailyExtractLimit',
    'monthlyExtractLimit',
    'monthlyRewriteLimit',
    'monthlyTranscriptionMinutes',
    'maxLinksPerJob'
  ];
  return allowed.reduce((result, key) => {
    const next = Number(value[key]);
    if (Number.isFinite(next) && next >= 0) {
      result[key] = Math.floor(next);
    }
    return result;
  }, {});
}

function normalizePaymentStatus(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) return 'pending_payment';
  const map = {
    pending: 'pending_payment',
    pending_manual_payment: 'pending_payment',
    unpaid: 'pending_payment',
    waiting: 'pending_payment',
    created: 'pending_payment',
    active: 'paid',
    paid: 'paid',
    success: 'paid',
    succeeded: 'paid',
    completed: 'paid',
    finished: 'paid',
    trade_success: 'paid',
    trade_finished: 'paid',
    transaction_success: 'paid',
    cancelled: 'cancelled',
    canceled: 'cancelled',
    closed: 'cancelled',
    expired: 'cancelled',
    failed: 'failed'
  };
  return map[normalized] || normalized;
}

function isPaidPaymentStatus(status) {
  return normalizePaymentStatus(status) === 'paid';
}

function normalizeBillingRecords(records = []) {
  if (!Array.isArray(records)) return [];
  return records
    .filter((record) => record && typeof record === 'object')
    .map((record) => ({
      id: String(record.id || randomUUID()),
      planId: normalizePlanId(record.planId),
      planName: PLAN_CATALOG[normalizePlanId(record.planId)].name,
      amount: Number(record.amount || 0),
      currency: String(record.currency || 'CNY'),
      status: normalizePaymentStatus(record.status || 'pending_payment'),
      provider: String(record.provider || PAYMENT_PROVIDER),
      channel: String(record.channel || ''),
      tradeNo: String(record.tradeNo || ''),
      orderNote: String(record.orderNote || `ClipFlow-${String(record.id || '').slice(0, 8).toUpperCase()}`),
      createdAt: String(record.createdAt || new Date().toISOString()),
      paidAt: String(record.paidAt || ''),
      updatedAt: String(record.updatedAt || record.paidAt || record.createdAt || new Date().toISOString())
    }))
    .slice(0, 100);
}

function normalizeUsageHistory(history = []) {
  if (!Array.isArray(history)) return [];
  return history
    .filter((entry) => entry && typeof entry === 'object')
    .map((entry) => ({
      id: String(entry.id || randomUUID()),
      jobId: String(entry.jobId || ''),
      status: String(entry.status || 'processing'),
      createdAt: String(entry.createdAt || new Date().toISOString()),
      updatedAt: String(entry.updatedAt || entry.createdAt || new Date().toISOString()),
      urlCount: Number(entry.urlCount || 0),
      successCount: Number(entry.successCount || 0),
      failedCount: Number(entry.failedCount || 0),
      urls: Array.isArray(entry.urls) ? entry.urls.map(String).slice(0, MAX_LINKS_PER_JOB) : [],
      titles: Array.isArray(entry.titles) ? entry.titles.map(String).slice(0, MAX_LINKS_PER_JOB) : [],
      descriptions: Array.isArray(entry.descriptions) ? entry.descriptions.map(String).slice(0, MAX_LINKS_PER_JOB) : [],
      coverUrls: Array.isArray(entry.coverUrls) ? entry.coverUrls.map(String).slice(0, MAX_LINKS_PER_JOB) : [],
      items: normalizeHistoryItems(entry.items),
      error: String(entry.error || '')
    }))
    .slice(0, USER_HISTORY_LIMIT);
}

function normalizeHistoryItems(items = []) {
  if (!Array.isArray(items)) return [];
  return items
    .filter((item) => item && typeof item === 'object')
    .map((item, index) => ({
      id: String(item.id || `item-${index + 1}`),
      inputUrl: String(item.inputUrl || ''),
      status: String(item.status || 'done'),
      title: String(item.title || ''),
      description: String(item.description || ''),
      scriptText: String(item.scriptText || ''),
      coverUrl: String(item.coverUrl || ''),
      author: String(item.author || ''),
      provider: String(item.provider || 'douyin'),
      publishedAt: String(item.publishedAt || ''),
      likeCount: item.likeCount === null || item.likeCount === undefined ? '' : String(item.likeCount),
      musicTitle: String(item.musicTitle || ''),
      error: String(item.error || '')
    }))
    .slice(0, MAX_LINKS_PER_JOB);
}

function normalizeAccountIdentifier(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error('请输入手机号或邮箱。');
  }
  if (raw.includes('@')) {
    const email = raw.toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      throw new Error('邮箱格式不正确。');
    }
    return { identifier: email, type: 'email' };
  }

  const digits = raw.replace(/[\s-]/g, '');
  const phone = digits.startsWith('+') ? digits : /^1\d{10}$/.test(digits) ? `+86${digits}` : digits;
  if (!/^\+?\d{6,20}$/.test(phone)) {
    throw new Error('手机号格式不正确。');
  }
  return { identifier: phone, type: 'phone' };
}

function normalizePhoneLogin(body = {}) {
  const countryCode = String(body.countryCode || '+86').trim().replace(/[^\d+]/g, '') || '+86';
  const phoneInput = String(body.phone || body.identifier || '').trim();
  if (!phoneInput) {
    throw new Error('请输入手机号。');
  }
  const normalizedInput = phoneInput.startsWith('+')
    ? phoneInput
    : `${countryCode}${phoneInput.replace(/^0+/, '')}`;
  const normalized = normalizeAccountIdentifier(normalizedInput);
  if (normalized.type !== 'phone') {
    throw new Error('请输入手机号。');
  }
  return normalized;
}

function findUserByIdentifier(users, identifier) {
  return users.find((user) => user.email === identifier || samePhone(user.phone, identifier));
}

function samePhone(left = '', right = '') {
  if (!left || !right) return false;
  const normalize = (value) => String(value).replace(/[^\d]/g, '').replace(/^86(?=1\d{10}$)/, '');
  return normalize(left) === normalize(right);
}

function safeDisplayName(value, fallback) {
  const cleaned = String(value || '').trim().replace(/\s+/g, ' ').slice(0, 32);
  if (cleaned) return cleaned;
  if (String(fallback || '').includes('@')) return String(fallback).split('@')[0].slice(0, 32) || '用户';
  const text = String(fallback || '');
  return text ? `用户${text.slice(-4)}` : '用户';
}

function isStrongPassword(password) {
  const text = String(password || '');
  return text.length >= 8 && /[A-Za-z]/.test(text) && /\d/.test(text);
}

async function hashPassword(password) {
  const salt = randomBytes(16).toString('base64url');
  const hash = await scryptAsync(password, salt, 64);
  return `scrypt:${salt}:${Buffer.from(hash).toString('base64url')}`;
}

async function verifyPassword(password, storedHash = '') {
  const [scheme, salt, hash] = String(storedHash).split(':');
  if (scheme !== 'scrypt' || !salt || !hash) return false;
  const expected = Buffer.from(hash, 'base64url');
  const actual = await scryptAsync(password, salt, expected.length);
  if (actual.length !== expected.length) return false;
  return timingSafeEqual(actual, expected);
}

function setSessionCookie(res, userId) {
  const expiresAt = Date.now() + SESSION_TTL_MS;
  const payload = Buffer.from(JSON.stringify({ userId, expiresAt })).toString('base64url');
  const signature = signSessionPayload(payload);
  const maxAge = Math.floor(SESSION_TTL_MS / 1000);
  const secure = process.env.NODE_ENV === 'production' ? '; Secure' : '';
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=${payload}.${signature}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${maxAge}${secure}`);
}

function clearSessionCookie(res) {
  const secure = process.env.NODE_ENV === 'production' ? '; Secure' : '';
  res.setHeader('Set-Cookie', `${SESSION_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0${secure}`);
}

function guestQuotaView(req) {
  const token = parseCookies(req)[GUEST_COOKIE];
  const usage = verifyGuestToken(token) || { used: 0 };
  return {
    used: clamp(Number(usage.used || 0), 0, 2),
    total: 2,
    remaining: Math.max(0, 2 - Number(usage.used || 0)),
    maxLinksPerJob: 1,
    requiresRegistration: Number(usage.used || 0) >= 2
  };
}

function consumeGuestQuota(res, used) {
  const next = {
    used: clamp(Number(used || 0), 0, 2),
    total: 2,
    remaining: Math.max(0, 2 - Number(used || 0)),
    maxLinksPerJob: 1,
    requiresRegistration: Number(used || 0) >= 2
  };
  const expiresAt = Date.now() + GUEST_TTL_MS;
  const payload = Buffer.from(JSON.stringify({ used: next.used, expiresAt })).toString('base64url');
  const signature = signSessionPayload(`guest:${payload}`);
  const secure = process.env.NODE_ENV === 'production' ? '; Secure' : '';
  const maxAge = Math.floor(GUEST_TTL_MS / 1000);
  res.append('Set-Cookie', `${GUEST_COOKIE}=${payload}.${signature}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${maxAge}${secure}`);
  return next;
}

function verifyGuestToken(token = '') {
  const [payload, signature] = String(token).split('.');
  if (!payload || !signature) return null;
  const expected = signSessionPayload(`guest:${payload}`);
  const left = Buffer.from(signature);
  const right = Buffer.from(expected);
  if (left.length !== right.length || !timingSafeEqual(left, right)) return null;
  try {
    const parsed = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    if (Number(parsed.expiresAt) < Date.now()) return null;
    return parsed;
  } catch {
    return null;
  }
}

function signSessionPayload(payload) {
  return createHmac('sha256', AUTH_SECRET).update(payload).digest('base64url');
}

function parseCookies(req) {
  return String(req.headers.cookie || '')
    .split(';')
    .map((part) => part.trim())
    .filter(Boolean)
    .reduce((cookies, part) => {
      const index = part.indexOf('=');
      if (index > -1) cookies[part.slice(0, index)] = decodeURIComponent(part.slice(index + 1));
      return cookies;
    }, {});
}

function verifySessionToken(token = '') {
  const [payload, signature] = String(token).split('.');
  if (!payload || !signature) return null;
  const expected = signSessionPayload(payload);
  const left = Buffer.from(signature);
  const right = Buffer.from(expected);
  if (left.length !== right.length || !timingSafeEqual(left, right)) return null;

  try {
    const parsed = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    if (!parsed.userId || Number(parsed.expiresAt) < Date.now()) return null;
    return parsed;
  } catch {
    return null;
  }
}

async function getRequestUser(req) {
  const token = parseCookies(req)[SESSION_COOKIE];
  const session = verifySessionToken(token);
  if (!session) return null;
  const db = await readUserDb();
  const user = db.users.find((item) => item.id === session.userId);
  if (!user || user.status !== 'active') return null;
  return user;
}

function publicUser(user) {
  if (!user) return null;
  return {
    id: user.id,
    displayName: user.displayName,
    accountType: user.accountType,
    email: user.email || '',
    phone: user.phone || '',
    role: user.role,
    status: user.status,
    planId: normalizePlanId(user.planId),
    createdAt: user.createdAt,
    lastLoginAt: user.lastLoginAt || ''
  };
}

function adminUserView(user) {
  const history = normalizeUsageHistory(user.history);
  return {
    ...publicUser(user),
    usageStats: normalizeUsageStats(user.usageStats),
    quota: quotaView(user),
    billingRecords: normalizeBillingRecords(user.billingRecords).slice(0, 5),
    recentFailures: history
      .filter((entry) => entry.status === 'failed' || entry.failedCount > 0 || entry.error)
      .slice(0, 3)
      .map((entry) => ({
        jobId: entry.jobId,
        updatedAt: entry.updatedAt,
        error: entry.error
      })),
    updatedAt: user.updatedAt || ''
  };
}

function userUsageView(user) {
  return {
    user: publicUser(user),
    stats: normalizeUsageStats(user.usageStats),
    quota: quotaView(user),
    billingRecords: normalizeBillingRecords(user.billingRecords).slice(0, 12),
    history: normalizeUsageHistory(user.history).map((entry) => ({
      id: entry.id,
      jobId: entry.jobId,
      status: entry.status,
      createdAt: entry.createdAt,
      updatedAt: entry.updatedAt,
      urlCount: entry.urlCount,
      successCount: entry.successCount,
      failedCount: entry.failedCount,
      urls: entry.urls,
      titles: entry.titles,
      descriptions: entry.descriptions,
      coverUrls: entry.coverUrls,
      items: entry.items,
      error: entry.error
    }))
  };
}

function historyDetailView(entry, liveJob = null) {
  if (liveJob) {
    return {
      id: entry.id,
      jobId: entry.jobId || liveJob.id,
      status: liveJob.status || entry.status,
      createdAt: liveJob.createdAt || entry.createdAt,
      updatedAt: liveJob.updatedAt || entry.updatedAt,
      urlCount: liveJob.items?.length || entry.urlCount,
      successCount: entry.successCount,
      failedCount: entry.failedCount,
      items: (liveJob.items || []).map((item, index) => publicHistoryItemFromJob(item, entry, index))
    };
  }

  const count = Math.max(entry.items.length, entry.urls.length, entry.titles.length, entry.descriptions.length, entry.urlCount || 0, 1);
  return {
    id: entry.id,
    jobId: entry.jobId,
    status: entry.status,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
    urlCount: entry.urlCount || count,
    successCount: entry.successCount,
    failedCount: entry.failedCount,
    items: Array.from({ length: count }, (_unused, index) => publicHistoryItemFromEntry(entry, index))
  };
}

function publicHistoryItemFromJob(item, entry, index) {
  const parsed = item.parsed || {};
  return {
    id: String(item.id || `item-${index + 1}`),
    inputUrl: String(item.inputUrl || entry.urls[index] || ''),
    status: String(item.status || entry.status),
    title: String(parsed.title || entry.titles[index] || ''),
    description: String(parsed.description || entry.descriptions[index] || parsed.title || ''),
    scriptText: String(item.scriptText || parsed.description || parsed.title || entry.descriptions[index] || ''),
    coverUrl: String(parsed.coverUrl || entry.coverUrls[index] || localHistoryCoverUrl(entry.jobId, index)),
    author: String(parsed.author || ''),
    provider: String(parsed.provider || 'douyin'),
    publishedAt: String(parsed.publishedAt || ''),
    likeCount: parsed.likeCount === null || parsed.likeCount === undefined ? '' : String(parsed.likeCount),
    musicTitle: String(parsed.musicTitle || ''),
    assets: item.assets || {},
    transcription: item.transcription || null,
    videoOptimization: item.videoOptimization || null,
    error: String(item.error || '')
  };
}

function publicHistoryItemFromEntry(entry, index) {
  const saved = entry.items[index] || {};
  return {
    id: String(saved.id || `item-${index + 1}`),
    inputUrl: String(saved.inputUrl || entry.urls[index] || ''),
    status: String(saved.status || entry.status),
    title: String(saved.title || entry.titles[index] || ''),
    description: String(saved.description || entry.descriptions[index] || saved.title || entry.titles[index] || ''),
    scriptText: String(saved.scriptText || saved.description || entry.descriptions[index] || saved.title || entry.titles[index] || ''),
    coverUrl: String(saved.coverUrl || entry.coverUrls[index] || localHistoryCoverUrl(entry.jobId, index)),
    author: String(saved.author || ''),
    provider: String(saved.provider || 'douyin'),
    publishedAt: String(saved.publishedAt || ''),
    likeCount: saved.likeCount === null || saved.likeCount === undefined ? '' : String(saved.likeCount || ''),
    musicTitle: String(saved.musicTitle || ''),
    assets: {},
    transcription: null,
    videoOptimization: null,
    error: String(saved.error || entry.error || '')
  };
}

function localHistoryCoverUrl(jobId, index) {
  return jobId ? `/tmp/jobs/${encodeURIComponent(jobId)}/item-${index + 1}/cover.webp` : '';
}

function quotaView(user) {
  const plan = effectivePlanForUser(user);
  const stats = normalizeUsageStats(user.usageStats);
  return {
    plan: {
      id: plan.id,
      name: plan.name,
      englishName: plan.englishName,
      description: plan.description,
      priceMonthly: plan.priceMonthly,
      currency: plan.currency,
      priority: plan.priority
    },
    extracts: {
      dailyLimit: plan.dailyExtractLimit,
      dailyUsed: stats.dailyExtracts,
      dailyRemaining: remainingQuota(plan.dailyExtractLimit, stats.dailyExtracts),
      monthlyLimit: plan.monthlyExtractLimit,
      monthlyUsed: stats.monthlyExtracts,
      monthlyRemaining: remainingQuota(plan.monthlyExtractLimit, stats.monthlyExtracts),
      maxLinksPerJob: plan.maxLinksPerJob
    },
    rewrites: {
      monthlyLimit: plan.monthlyRewriteLimit,
      monthlyUsed: stats.monthlyRewrites,
      monthlyRemaining: Math.max(0, plan.monthlyRewriteLimit - stats.monthlyRewrites)
    },
    transcription: {
      monthlyMinutesLimit: plan.monthlyTranscriptionMinutes,
      monthlySecondsUsed: stats.monthlyTranscriptionSeconds,
      monthlyMinutesUsed: Math.ceil(stats.monthlyTranscriptionSeconds / 60),
      monthlyMinutesRemaining: remainingQuota(plan.monthlyTranscriptionMinutes, Math.ceil(stats.monthlyTranscriptionSeconds / 60))
    },
    period: stats.usagePeriod,
    day: stats.usageDay
  };
}

function remainingQuota(limit, used) {
  if (limit === null || limit === undefined) return null;
  return Math.max(0, Number(limit || 0) - Number(used || 0));
}

function effectivePlanForUser(user = {}) {
  const base = PLAN_CATALOG[normalizePlanId(user.planId)] || PLAN_CATALOG.free;
  const overrides = normalizeQuotaOverrides(user.quotaOverrides);
  return {
    ...base,
    ...overrides,
    maxLinksPerJob: Math.min(
      Number(overrides.maxLinksPerJob || base.maxLinksPerJob || MAX_LINKS_PER_JOB),
      MAX_LINKS_PER_JOB
    )
  };
}

function planCatalogView() {
  return Object.values(PLAN_CATALOG).map((plan) => ({
    ...plan,
    maxLinksPerJob: Math.min(plan.maxLinksPerJob, MAX_LINKS_PER_JOB)
  }));
}

function normalizePemKey(raw, type = 'PRIVATE KEY') {
  const text = String(raw || '').trim();
  if (!text) return '';
  if (text.includes('BEGIN')) return text;
  const cleaned = text.replace(/\s+/g, '');
  const rows = cleaned.match(/.{1,64}/g) || [];
  return `-----BEGIN ${type}-----\n${rows.join('\n')}\n-----END ${type}-----`;
}

function isAlipayConfigured() {
  return Boolean(ALIPAY_APP_ID && ALIPAY_PRIVATE_KEY && ALIPAY_PUBLIC_KEY && PAYMENT_WEBHOOK_SECRET);
}

function getAlipayClient() {
  if (alipayClient) return alipayClient;
  if (!ALIPAY_APP_ID) {
    throw new Error('ALIPAY_APP_ID 未配置');
  }
  const privateKeyPem = normalizePemKey(ALIPAY_PRIVATE_KEY, 'PRIVATE KEY');
  const alipayPublicKeyPem = normalizePemKey(ALIPAY_PUBLIC_KEY, 'PUBLIC KEY');
  alipayClient = new AlipaySdk({
    appId: ALIPAY_APP_ID,
    privateKey: privateKeyPem,
    alipayPublicKey: alipayPublicKeyPem,
    gateway: ALIPAY_GATEWAY,
    keyType: 'PKCS8',
    timeout: 15000
  });
  return alipayClient;
}

async function createAlipayPrecreateOrder({ payment, plan }) {
  const client = getAlipayClient();
  const amount = Number(payment.amount || plan.priceMonthly || 0).toFixed(2);
  const subject = `ClipFlow-${plan.name}-${payment.id.slice(0, 8).toUpperCase()}`;
  const response = await client.exec('alipay.trade.precreate', {
    notifyUrl: ALIPAY_NOTIFY_URL,
    bizContent: {
      outTradeNo: payment.id,
      totalAmount: amount,
      subject,
      body: `${plan.name} membership`,
      timeoutExpress: '30m'
    }
  });
  const payload = response?.alipayTradePrecreateResponse || response?.alipay_trade_precreate_response || response || {};
  if (payload.code !== '10000') {
    const detail = payload.subMsg || payload.msg || payload.code || 'unknown';
    throw new Error(detail);
  }
  const qrCode = String(payload.qrCode || payload.qr_code || '').trim();
  if (!qrCode) {
    throw new Error('支付宝未返回二维码链接');
  }
  const qrDataUrl = await QRCode.toDataURL(qrCode, { margin: 1, width: 520 });
  return {
    qrCode,
    qrDataUrl,
    outTradeNo: payment.id,
    tradeNo: String(payload.tradeNo || payload.trade_no || '')
  };
}

function manualPaymentView(payment, plan) {
  if (PAYMENT_PROVIDER !== 'manual') return null;
  return {
    mode: 'manual_qr',
    receiverName: PAYMENT_RECEIVER_NAME,
    amount: payment.amount,
    currency: payment.currency,
    planId: plan.id,
    planName: plan.name,
    orderId: payment.id,
    orderNote: payment.orderNote || `ClipFlow-${payment.id.slice(0, 8).toUpperCase()}`,
    channels: [
      {
        id: 'wechat',
        name: '微信支付',
        qrUrl: PAYMENT_WECHAT_QR_URL,
        enabled: Boolean(PAYMENT_WECHAT_QR_URL)
      },
      {
        id: 'alipay',
        name: '支付宝',
        qrUrl: PAYMENT_ALIPAY_QR_URL,
        enabled: Boolean(PAYMENT_ALIPAY_QR_URL)
      }
    ],
    autoUpgradeEnabled: Boolean(PAYMENT_WEBHOOK_SECRET),
    notice: PAYMENT_WEBHOOK_SECRET
      ? '支付成功后系统会自动升级套餐，通常在 3-10 秒内生效。'
      : '当前未配置支付回调，付款后请联系管理员确认订单。'
  };
}

function alipayCheckoutView(payment, plan, precreate) {
  return {
    provider: 'alipay',
    receiverName: PAYMENT_RECEIVER_NAME,
    amount: payment.amount,
    currency: payment.currency,
    planId: plan.id,
    planName: plan.name,
    orderId: payment.id,
    orderNote: payment.orderNote || `ClipFlow-${payment.id.slice(0, 8).toUpperCase()}`,
    channels: [
      {
        id: 'alipay',
        label: '支付宝',
        qrUrl: precreate.qrDataUrl,
        enabled: true
      }
    ],
    autoUpgradeEnabled: Boolean(PAYMENT_WEBHOOK_SECRET),
    notice: '请使用支付宝扫码支付。支付成功后系统会自动升级套餐。'
  };
}

function isPaymentWebhookAuthorized(req) {
  if (!PAYMENT_WEBHOOK_SECRET) return false;
  const tokenFromQuery = String(req.query?.token || '');
  const tokenFromBody = String(req.body?.token || req.body?.webhookToken || '');
  const tokenFromHeader = String(req.headers['x-payment-secret'] || req.headers['x-webhook-secret'] || '');
  const supplied = tokenFromHeader || tokenFromBody || tokenFromQuery;
  return supplied === PAYMENT_WEBHOOK_SECRET;
}

function isLikelyAlipayNotify(req) {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  return PAYMENT_PROVIDER === 'alipay'
    || Boolean(body.sign)
    || Boolean(body.trade_status)
    || Boolean(body.out_trade_no);
}

function verifyAlipayNotifySignature(req) {
  try {
    const body = req.body && typeof req.body === 'object' ? req.body : {};
    if (!body.sign || !body.sign_type) return false;
    const client = getAlipayClient();
    return client.checkNotifySignV2(body) || client.checkNotifySign(body, true);
  } catch {
    return false;
  }
}

function pickWebhookField(obj = {}, keys = []) {
  for (const key of keys) {
    const value = obj[key];
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      return String(value).trim();
    }
  }
  return '';
}

function normalizeWebhookPayload(req) {
  const source = (req.body && typeof req.body === 'object') ? req.body : {};
  const nested = (source.data && typeof source.data === 'object') ? source.data : {};
  const merged = { ...nested, ...source };
  const orderId = pickWebhookField(merged, [
    'orderId',
    'paymentId',
    'payment_id',
    'id',
    'order_id',
    'orderNo',
    'order_no',
    'outTradeNo',
    'out_trade_no',
    'merchant_order_no'
  ]);
  const rawStatus = pickWebhookField(merged, ['status', 'trade_status', 'state', 'payment_status', 'result']);
  const status = normalizePaymentStatus(rawStatus);
  const amountRaw = pickWebhookField(merged, ['amount', 'money', 'total_amount', 'total_fee', 'receipt_amount']);
  const amount = Number(amountRaw || 0);
  const tradeNo = pickWebhookField(merged, ['tradeNo', 'trade_no', 'transaction_id', 'provider_trade_no']);
  const channel = pickWebhookField(merged, ['channel', 'type', 'pay_type', 'payment_channel']);
  const provider = pickWebhookField(merged, ['provider']) || (merged.trade_status ? 'alipay' : PAYMENT_PROVIDER);
  return {
    orderId,
    status,
    isPaid: isPaidPaymentStatus(status),
    amount: Number.isFinite(amount) ? amount : 0,
    tradeNo,
    channel,
    provider,
    rawPayload: source
  };
}

async function markOrderPaidAndUpgrade(payload) {
  const db = await readUserDb();
  const now = new Date().toISOString();
  const orderId = String(payload.orderId || '');

  for (const user of db.users) {
    const records = normalizeBillingRecords(user.billingRecords);
    const record = records.find((item) => item.id === orderId);
    if (!record) continue;

    if (!isPaidPaymentStatus(record.status)) {
      record.status = 'paid';
      record.provider = payload.provider || record.provider;
      record.channel = payload.channel || record.channel;
      record.tradeNo = payload.tradeNo || record.tradeNo;
      if (Number.isFinite(payload.amount) && payload.amount > 0) {
        record.amount = payload.amount;
      }
      record.paidAt = now;
      record.updatedAt = now;
    }

    if (user.role !== 'admin') {
      user.planId = normalizePlanId(record.planId);
    }
    user.billingRecords = records;
    user.updatedAt = now;
    await writeUserDb(db);
    return { ok: true, userId: user.id, planId: user.planId };
  }

  return { ok: false };
}

function requireLoggedInUser(req) {
  return getRequestUser(req);
}

function assertQuota(user, type, amount = 1) {
  const quota = quotaView(user);
  if (type === 'extract') {
    if (amount > quota.extracts.maxLinksPerJob) {
      throw Object.assign(new Error(batchLimitMessage(user, quota.extracts.maxLinksPerJob)), { statusCode: 402 });
    }
    if (quota.extracts.dailyRemaining !== null && quota.extracts.dailyRemaining < amount) {
      throw Object.assign(new Error('今日解析额度已用完，升级标准版可解锁无日限制和每月 1500 次额度。'), { statusCode: 402 });
    }
    if (quota.extracts.monthlyRemaining !== null && quota.extracts.monthlyRemaining < amount) {
      const message = user.planId === 'standard'
        ? '本月标准版额度已用完，升级高级版可解锁每月 10000 次和优先解析通道。'
        : '本月额度已用完，可升级标准版解锁每月 1500 次解析额度。';
      throw Object.assign(new Error(message), { statusCode: 402 });
    }
  }
  if (type === 'rewrite' && quota.rewrites.monthlyRemaining < amount) {
    throw Object.assign(new Error('本月 AI 改写额度不足，请升级套餐。'), { statusCode: 402 });
  }
}

function batchLimitMessage(user, limit) {
  if (user?.planId === 'free') {
    return '免费版每次最多支持解析 2 个链接，升级标准版可解锁最多 5 个链接的批量处理。';
  }
  return `当前套餐每次最多支持解析 ${limit} 个链接。`;
}

function buildGuestQuotaNotice(quota) {
  if (!quota) return null;
  if (quota.used >= quota.total) {
    return {
      type: 'guest_exhausted',
      message: '您的免费体验额度已用完，注册账号即可继续使用免费版权益：每日 5 次解析、每月最高 150 次、批量处理 2 个链接。',
      action: 'register'
    };
  }
  return {
    type: 'guest_used',
    message: `您已使用第 ${quota.used}/${quota.total} 次免费体验额度。`,
    action: ''
  };
}

function buildExtractQuotaNotice(user, quota) {
  const planId = user?.planId || quota?.plan?.id;
  if (!quota?.extracts) return null;
  if (planId === 'free') {
    if (quota.extracts.monthlyRemaining === 0) {
      return {
        type: 'free_monthly_exhausted',
        message: '本月额度已用完，可升级标准版解锁每月 1500 次解析额度。',
        action: 'upgrade_standard'
      };
    }
    if (quota.extracts.dailyRemaining === 0) {
      return {
        type: 'free_daily_exhausted',
        message: '今日解析额度已用完，升级标准版可解锁无日限制。',
        action: 'upgrade_standard'
      };
    }
    if (quota.extracts.dailyRemaining === 1) {
      return {
        type: 'free_daily_warning',
        message: '今日还剩 1 次解析额度，升级标准版可解锁无日限制。',
        action: 'upgrade_standard'
      };
    }
    if (quota.extracts.monthlyUsed >= 120 && quota.extracts.monthlyRemaining > 0) {
      return {
        type: 'free_monthly_warning',
        message: `本月解析额度还剩 ${quota.extracts.monthlyRemaining} 次，升级标准版可解锁 1500 次额度。`,
        action: 'upgrade_standard'
      };
    }
  }
  if (planId === 'standard' && quota.extracts.monthlyRemaining !== null && quota.extracts.monthlyRemaining <= 150) {
    return {
      type: 'standard_monthly_warning',
      message: '标准版额度接近上限，升级高级版可解锁每月 10000 次、优先解析通道和专属客服支持。',
      action: 'upgrade_pro'
    };
  }
  return null;
}

async function consumeUserQuota(userId, type, amount = 1) {
  const db = await readUserDb();
  const user = db.users.find((item) => item.id === userId);
  if (!user) return null;
  user.usageStats = normalizeUsageStats(user.usageStats);
  assertQuota(user, type, amount);
  if (type === 'extract') {
    user.usageStats.monthlyExtracts += amount;
    user.usageStats.dailyExtracts += amount;
  }
  if (type === 'rewrite') {
    user.usageStats.monthlyRewrites += amount;
  }
  user.usageStats.lastUsedAt = new Date().toISOString();
  user.updatedAt = user.usageStats.lastUsedAt;
  await writeUserDb(db);
  return quotaView(user);
}

async function recordJobSubmitted(userId, job, urls) {
  const db = await readUserDb();
  const user = db.users.find((item) => item.id === userId);
  if (!user) return;

  const now = new Date().toISOString();
  user.usageStats = normalizeUsageStats(user.usageStats);
  user.usageStats.totalJobs += 1;
  user.usageStats.totalLinks += urls.length;
  user.usageStats.lastUsedAt = now;
  user.history = normalizeUsageHistory(user.history);
  user.history.unshift({
    id: randomUUID(),
    jobId: job.id,
    status: 'processing',
    createdAt: now,
    updatedAt: now,
    urlCount: urls.length,
    successCount: 0,
    failedCount: 0,
    urls,
    titles: [],
    descriptions: [],
    coverUrls: [],
    items: [],
    error: ''
  });
  user.history = user.history.slice(0, USER_HISTORY_LIMIT);
  user.updatedAt = now;
  await writeUserDb(db);
}

async function recordJobCompleted(job) {
  const db = await readUserDb();
  const user = db.users.find((item) => item.id === job.userId);
  if (!user) return;

  const now = new Date().toISOString();
  const doneItems = job.items.filter((item) => item.status === 'done');
  const failedItems = job.items.filter((item) => item.status === 'failed');
  user.usageStats = normalizeUsageStats(user.usageStats);
  user.usageStats.successItems += doneItems.length;
  user.usageStats.failedItems += failedItems.length;
  user.usageStats.lastUsedAt = now;

  user.history = normalizeUsageHistory(user.history);
  const entry = user.history.find((item) => item.jobId === job.id);
  if (entry) {
    entry.status = job.status;
    entry.updatedAt = now;
    entry.successCount = doneItems.length;
    entry.failedCount = failedItems.length;
    entry.titles = doneItems.map((item) => item.parsed?.title || item.inputUrl).slice(0, MAX_LINKS_PER_JOB);
    entry.descriptions = doneItems.map((item) => item.parsed?.description || item.parsed?.title || item.inputUrl).slice(0, MAX_LINKS_PER_JOB);
    entry.coverUrls = doneItems.map((item) => item.parsed?.coverUrl || '').slice(0, MAX_LINKS_PER_JOB);
    entry.items = doneItems.map((item, index) => historyItemFromCompletedJobItem(item, index)).slice(0, MAX_LINKS_PER_JOB);
    entry.error = failedItems.map((item) => item.error).filter(Boolean).join('；').slice(0, 300);
  }
  user.updatedAt = now;
  await writeUserDb(db);
}

function historyItemFromCompletedJobItem(item, index) {
  const parsed = item.parsed || {};
  return {
    id: String(item.id || `item-${index + 1}`),
    inputUrl: String(item.inputUrl || ''),
    status: String(item.status || 'done'),
    title: String(parsed.title || item.inputUrl || ''),
    description: String(parsed.description || parsed.title || ''),
    scriptText: String(item.scriptText || parsed.description || parsed.title || ''),
    coverUrl: String(parsed.coverUrl || ''),
    author: String(parsed.author || ''),
    provider: String(parsed.provider || 'douyin'),
    publishedAt: String(parsed.publishedAt || ''),
    likeCount: parsed.likeCount === null || parsed.likeCount === undefined ? '' : String(parsed.likeCount),
    musicTitle: String(parsed.musicTitle || ''),
    error: String(item.error || '')
  };
}

function normalizeRequestUrls(body = {}) {
  const raw = Array.isArray(body.urls)
    ? body.urls.join('\n')
    : String(body.text || body.url || '');
  const found = raw.match(/https?:\/\/[^\s，,。]+/g) || [];
  const deduped = [...new Set(found.map((url) => url.replace(/[)\]}。,.，]+$/, '')))];
  return deduped.map(validateDouyinUrl);
}

function validateDouyinUrl(input) {
  let parsed;
  try {
    parsed = new URL(input);
  } catch {
    throw new Error(`无效链接：${input}`);
  }
  const host = parsed.hostname.toLowerCase();
  const allowed = allowedHostSuffixes.some((suffix) => host === suffix || host.endsWith(`.${suffix}`));
  if (!allowed) {
    throw new Error(`仅支持抖音链接：${input}`);
  }
  return parsed.toString();
}

function createJob(urls, userId = '') {
  const now = Date.now();
  return {
    id: randomUUID(),
    userId,
    status: 'queued',
    progress: 0,
    createdAt: now,
    updatedAt: now,
    items: urls.map((url, index) => ({
      id: `item-${index + 1}`,
      inputUrl: url,
      status: 'queued',
      stage: '等待解析',
      progress: 0,
      assets: {}
    }))
  };
}

async function processJob(job) {
  job.status = 'processing';
  job.progress = 4;
  touch(job);

  for (let index = 0; index < job.items.length; index += 1) {
    const item = job.items[index];
    try {
      item.status = 'parsing';
      item.stage = '正在解析抖音链接';
      updateJobProgress(job, index, 0.12);

      const parsed = await parseDouyinFast(item.inputUrl);
      item.parsed = parsed;
      item.parseMs = Number(parsed.parseMs || 0);
      item.scriptText = buildScriptText(parsed);

      const itemDir = join(TMP_ROOT, job.id, item.id);
      await mkdir(itemDir, { recursive: true });

      let sourceVideoAssetId = '';
      if (parsed.coverUrl) {
        item.assets.cover = registerRemoteAsset(job, item, 'cover', parsed.coverUrl, `${safeFilename(parsed.title)}-cover.jpg`, 'image/jpeg');
      }
      if (parsed.videoUrl) {
        sourceVideoAssetId = registerRemoteAsset(job, item, 'source-video', parsed.videoUrl, `${safeFilename(parsed.title)}-source.mp4`, 'video/mp4');
        item.assets.video = sourceVideoAssetId;
      }
      if (parsed.musicUrl) {
        item.assets.bgm = registerRemoteAsset(job, item, 'bgm', parsed.musicUrl, `${safeFilename(parsed.musicTitle || parsed.title)}-bgm.mp3`, 'audio/mpeg');
      }

      item.status = 'media';
      item.stage = ENABLE_VIDEO_OPTIMIZE ? '正在生成高清优化视频' : '正在生成 MP3';
      updateJobProgress(job, index, 0.42);

      const videoAsset = assets.get(sourceVideoAssetId || item.assets.video);
      let mp3Error = '';
      if (videoAsset) {
        try {
          await ensureAssetLocal(videoAsset, job.id, item.id, 'source.mp4');
          if (!item.assets.cover) {
            try {
              const coverPath = join(itemDir, 'cover.webp');
              await runFfmpegCover(videoAsset.localPath, coverPath);
              item.assets.cover = registerLocalAsset(job, item, 'cover', coverPath, `${safeFilename(parsed.title)}-cover.webp`, 'image/webp');
              parsed.coverUrl = localHistoryCoverUrl(job.id, index);
            } catch (error) {
              item.coverError = error.message || 'Cover thumbnail generation failed.';
            }
          }
          if (ENABLE_VIDEO_OPTIMIZE) {
            try {
              const optimizedPath = join(itemDir, 'video-hd.mp4');
              await runFfmpegVideoOptimize(videoAsset.localPath, optimizedPath);
              item.assets.video = registerLocalAsset(job, item, 'video', optimizedPath, `${safeFilename(parsed.title)}-hd.mp4`, 'video/mp4');
              item.videoOptimization = {
                status: 'ok',
                crf: VIDEO_OPTIMIZE_CRF,
                preset: VIDEO_OPTIMIZE_PRESET
              };
            } catch (error) {
              item.videoOptimization = {
                status: 'skipped',
                reason: error.message || '高清优化失败，已保留原始无水印视频。'
              };
            }
          }

          item.stage = '正在生成 MP3';
          updateJobProgress(job, index, 0.62);
          const mp3Path = join(itemDir, 'audio.mp3');
          await runFfmpeg(videoAsset.localPath, mp3Path);
          item.assets.mp3 = registerLocalAsset(job, item, 'mp3', mp3Path, `${safeFilename(parsed.title)}.mp3`, 'audio/mpeg');
        } catch (error) {
          mp3Error = error.message || 'MP3 生成失败。';
        }
      }

      item.status = 'transcribing';
      item.stage = '正在生成文案转写';
      updateJobProgress(job, index, 0.72);

      const mp3Asset = assets.get(item.assets.mp3);
      if (mp3Asset) {
        const transcription = await transcribeIfConfigured(mp3Asset.localPath);
        if (transcription.status === 'ok') {
          const transcriptPath = join(itemDir, 'transcript.txt');
          await writeFile(transcriptPath, transcription.text, 'utf8');
          item.assets.transcript = registerLocalAsset(job, item, 'transcript', transcriptPath, `${safeFilename(parsed.title)}-transcript.txt`, 'text/plain; charset=utf-8');
          item.transcription = { status: 'ok' };
          item.scriptText = firstText(transcription.text, item.scriptText);
        } else {
          item.transcription = transcription;
        }
      } else {
        item.transcription = {
          status: 'skipped',
          reason: mp3Error ? `MP3 生成失败：${mp3Error}` : '未生成 MP3，跳过转写。'
        };
      }

      item.status = 'done';
      item.stage = '已完成';
      updateJobProgress(job, index, 1);
    } catch (error) {
      item.status = 'failed';
      item.stage = '解析失败';
      item.error = error.message || '未知错误';
      updateJobProgress(job, index, 1);
    }
  }

  const doneCount = job.items.filter((item) => item.status === 'done').length;
  const failedCount = job.items.filter((item) => item.status === 'failed').length;
  job.status = doneCount === job.items.length ? 'done' : failedCount === job.items.length ? 'failed' : 'partial';
  job.progress = 100;
  touch(job);
  if (job.userId) {
    await recordJobCompleted(job);
  }
}

async function parseDouyin(url) {
  return parseDouyinFast(url);
  const providers = getEnabledProviders();
  const candidateUrls = await getCandidateUrls(url);
  const failures = [];

  for (const provider of providers) {
    if (!provider.isConfigured()) {
      failures.push(`${provider.providerName}: 未配置，已跳过`);
      continue;
    }

    const providerFailures = [];
    for (const candidateUrl of candidateUrls) {
      try {
        const parsed = await provider(candidateUrl, { originalUrl: url, candidateUrls });
        return {
          ...parsed,
          sourceUrl: url,
          resolvedUrl: candidateUrl
        };
      } catch (error) {
        providerFailures.push(error.message);
      }
    }
    failures.push(`${provider.providerName}: ${summarizeFailures(providerFailures)}`);
  }

  throw new Error(`解析服务不可用。${failures.slice(0, 8).join('；')}`);
}

async function getCandidateUrls(url) {
  // legacy entry kept for compatibility
  return getCandidateUrlsFast(url);
}

async function parseDouyinFast(url) {
  const providers = getEnabledProviders();
  const candidateUrls = await getCandidateUrlsFast(url);
  const cacheHit = getCachedParseResult([url, ...candidateUrls]);
  if (cacheHit) {
    return {
      ...cacheHit,
      sourceUrl: url,
      cached: true
    };
  }

  const failures = [];
  const startedAt = Date.now();
  const attempts = [];
  for (const provider of providers) {
    if (!provider.isConfigured()) {
      failures.push(`${provider.providerName}: 未配置，已跳过`);
      continue;
    }

    for (const candidateUrl of candidateUrls) {
      attempts.push({ provider, candidateUrl });
    }
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

async function getCandidateUrlsFast(url) {
  const urls = [url];
  try {
    const expanded = await Promise.race([
      expandRedirects(url),
      wait(REDIRECT_FAST_BUDGET_MS).then(() => [])
    ]);
    urls.push(...expanded);
  } catch {
    // Some short-link redirects block server requests. Keep the original URL.
  }
  return [...new Set(urls)].slice(0, MAX_CANDIDATE_URLS);
}

function wait(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function expandRedirects(url) {
  const urls = [];
  let current = url;
  for (let i = 0; i < REDIRECT_MAX_HOPS; i += 1) {
    const response = await fetchWithTimeout(current, {
      method: 'GET',
      redirect: 'manual',
      headers: requestHeaders()
    }, REDIRECT_TIMEOUT_MS);
    const location = response.headers.get('location');
    if (!location || response.status < 300 || response.status > 399) break;
    current = new URL(location, current).toString();
    urls.push(current);
  }
  return urls.map(validateDouyinUrl);
}

async function parseWithXinyew(url) {
  const endpoint = new URL('https://api.xinyew.cn/api/douyinjx');
  endpoint.searchParams.set('url', url);
  const json = await fetchJson(endpoint);
  const data = json.data;
  const extra = Array.isArray(data?.additional_data) ? data.additional_data[0] : {};
  const videoUrl = firstUrl(data?.nwm_video_url, data?.no_watermark_url, data?.play_url, data?.video_url, data?.url);
  if (!isSuccessCode(json.code) || !videoUrl) {
    throw new Error(json.msg || '新野 API 未返回视频地址。');
  }
  return {
    provider: 'Xinyew',
    title: firstText(data?.title, extra?.desc, 'douyin-video'),
    author: firstText(data?.author, extra?.nickname, ''),
    avatarUrl: firstUrl(extra?.url, data?.avatar),
    description: firstText(extra?.desc, data?.desc, data?.title, ''),
    coverUrl: firstUrl(data?.cover, data?.poster),
    videoUrl,
    musicUrl: firstUrl(data?.music?.url, data?.music_url),
    musicTitle: firstText(data?.music?.title, '')
  };
}
parseWithXinyew.providerName = 'Xinyew';
parseWithXinyew.providerKey = 'xinyew';
parseWithXinyew.isConfigured = () => true;

async function parseWithMxin(url) {
  const endpoint = new URL('https://api.mxin.moe/api/v1/douyin');
  endpoint.searchParams.set('url', url);
  const json = await fetchJson(endpoint);
  const data = json.data;
  const videoUrl = firstUrl(data?.url, data?.video_url, data?.video);
  if (!isSuccessCode(json.code) || !videoUrl) {
    throw new Error(json.msg || 'Mxin 未返回视频地址。');
  }
  return {
    provider: 'Mxin',
    title: firstText(data?.title, data?.desc, 'douyin-video'),
    author: firstText(data?.author, data?.nickname, ''),
    avatarUrl: firstUrl(data?.avatar, data?.avatar_url),
    description: firstText(data?.desc, data?.title, ''),
    coverUrl: firstUrl(data?.cover, data?.poster),
    videoUrl,
    musicUrl: firstUrl(data?.music?.url, data?.music_url),
    musicTitle: firstText(data?.music?.title, '')
  };
}
parseWithMxin.providerName = 'Mxin';
parseWithMxin.providerKey = 'mxin';
parseWithMxin.isConfigured = () => true;

async function parseWithTikHub(url) {
  const baseUrl = process.env.TIKHUB_BASE_URL || 'https://api.tikhub.io';
  const endpoints = [
    '/api/v1/douyin/app/v3/fetch_one_video_by_share_url',
    '/api/v1/douyin/web/fetch_one_video_by_share_url'
  ];
  const failures = [];

  for (const path of endpoints) {
    try {
      const endpoint = new URL(path, baseUrl);
      endpoint.searchParams.set('share_url', url);
      const json = await fetchJson(endpoint, {
        headers: {
          Authorization: `Bearer ${process.env.TIKHUB_API_KEY}`
        }
      });
      return normalizeTikHubResponse(json);
    } catch (error) {
      failures.push(`${path}: ${error.message}`);
    }
  }

  throw new Error(summarizeFailures(failures));
}
parseWithTikHub.providerName = 'TikHub';
parseWithTikHub.providerKey = 'tikhub';
parseWithTikHub.isConfigured = () => Boolean(process.env.TIKHUB_API_KEY);

function normalizeTikHubResponse(json) {
  const data = unwrapTikHubData(json);
  const videoUrl = firstUrl(
    data?.nwm_video_url,
    data?.no_watermark_url,
    data?.video?.nwm_video_url,
    data?.video?.no_watermark_url,
    preferredVideoUrls(data?.video?.play_addr_h264?.url_list),
    bestBitRatePlayUrls(data?.video?.bit_rate),
    preferredVideoUrls(data?.video?.play_addr?.url_list),
    preferredVideoUrls(data?.video?.play_addr_265?.url_list),
    preferredVideoUrls(data?.video?.play_addr_lowbr?.url_list),
    data?.video_url,
    data?.video_urls,
    data?.url,
    ALLOW_WATERMARK_FALLBACK ? preferredVideoUrls(data?.video?.download_addr?.url_list) : null
  );
  if (!videoUrl) {
    throw new Error(json.message || json.msg || 'TikHub 未返回无水印视频地址。');
  }
  return {
    provider: 'TikHub',
    title: firstText(data?.desc, data?.title, 'douyin-video'),
    author: firstText(data?.author?.nickname, data?.author_name, data?.nickname, ''),
    avatarUrl: firstUrl(data?.author?.avatar_thumb?.url_list, data?.author?.avatar_medium?.url_list),
    description: firstText(data?.desc, data?.title, ''),
    coverUrl: firstUrl(data?.video?.cover?.url_list, data?.video?.origin_cover?.url_list, data?.cover),
    videoUrl,
    musicUrl: firstUrl(data?.music?.play_url?.url_list, data?.music?.url, data?.music_urls),
    musicTitle: firstText(data?.music?.title, data?.music_title, ''),
    publishedAt: firstText(data?.create_time, ''),
    likeCount: data?.statistics?.digg_count ?? ''
  };
}

async function parseWithMakuo(url) {
  const endpoint = new URL('https://api.makuo.cc/api/get.video.douyin');
  endpoint.searchParams.set('url', url);
  endpoint.searchParams.set('token', process.env.MAKUO_TOKEN);
  const json = await fetchJson(endpoint, {
    headers: {
      Authorization: process.env.MAKUO_TOKEN
    }
  });
  const data = json.data;
  const videoUrl = firstUrl(data?.nwm_video_url, data?.no_watermark_url, data?.play_url, data?.video_url, data?.url);
  if (!isSuccessCode(json.code) || !videoUrl) {
    throw new Error(json.msg || 'Makuo 未返回视频地址。');
  }
  return {
    provider: 'Makuo',
    title: firstText(data?.title, 'douyin-video'),
    author: firstText(data?.author, ''),
    avatarUrl: firstUrl(data?.avatar, data?.music_Avatar),
    description: firstText(data?.title, ''),
    coverUrl: firstUrl(data?.cover),
    videoUrl,
    musicUrl: firstUrl(data?.music_bgm),
    musicTitle: firstText(data?.music_Name, ''),
    publishedAt: firstText(data?.time, ''),
    likeCount: data?.like ?? ''
  };
}
parseWithMakuo.providerName = 'Makuo';
parseWithMakuo.providerKey = 'makuo';
parseWithMakuo.isConfigured = () => Boolean(process.env.MAKUO_TOKEN);

async function parseWithMujie(url) {
  const endpoint = new URL('https://api.mu-jie.cc/douyin');
  endpoint.searchParams.set('url', url);
  endpoint.searchParams.set('key', process.env.MUJIE_KEY);
  const json = await fetchJson(endpoint);
  const data = json.data;
  const videoUrl = firstUrl(data?.nwm_video_url, data?.no_watermark_url, data?.play_url, data?.video_url, data?.url);
  if (!isSuccessCode(json.code) || !videoUrl) {
    throw new Error(json.msg || 'MuJie 未返回视频地址。');
  }
  return {
    provider: 'MuJie',
    title: firstText(data?.title, 'douyin-video'),
    author: firstText(data?.author, ''),
    avatarUrl: firstUrl(data?.avatar),
    description: firstText(data?.title, ''),
    coverUrl: firstUrl(data?.cover),
    videoUrl,
    musicUrl: firstUrl(data?.music?.url),
    musicTitle: firstText(data?.music?.title, ''),
    publishedAt: firstText(data?.time, ''),
    likeCount: data?.like ?? ''
  };
}
parseWithMujie.providerName = 'MuJie';
parseWithMujie.providerKey = 'mujie';
parseWithMujie.isConfigured = () => Boolean(process.env.MUJIE_KEY);

async function parseWithJxcxin(url) {
  const endpoint = new URL('https://apis.jxcxin.cn/api/douyin');
  endpoint.searchParams.set('url', url);
  const json = await fetchJson(endpoint);
  const data = json.data;
  const videoUrl = firstUrl(
    data?.nwm_video_url,
    data?.no_watermark_url,
    data?.play,
    data?.video?.play_addr?.url_list,
    data?.aweme_detail?.video?.play_addr?.url_list,
    data?.url,
    data?.video,
    data?.video_url,
    data?.video?.url
  );
  if (!videoUrl) {
    throw new Error(json.msg || 'Jxcxin 未返回视频地址。');
  }
  return {
    provider: 'Jxcxin',
    title: firstText(data?.title, data?.desc, data?.aweme_detail?.desc, 'douyin-video'),
    author: firstText(data?.author, data?.nickname, data?.user?.nickname, data?.aweme_detail?.author?.nickname, ''),
    avatarUrl: firstUrl(data?.avatar, data?.avatar_url, data?.user?.avatar, data?.aweme_detail?.author?.avatar_thumb?.url_list),
    description: firstText(data?.signature, data?.desc, data?.title, ''),
    coverUrl: firstUrl(data?.cover, data?.cover_url, data?.pic, data?.origin_cover, data?.video?.cover, data?.aweme_detail?.video?.cover?.url_list),
    videoUrl,
    musicUrl: firstUrl(data?.music?.url, data?.music_url, data?.music?.play_url?.url_list, data?.aweme_detail?.music?.play_url?.url_list),
    musicTitle: firstText(data?.music?.title, data?.aweme_detail?.music?.title, ''),
    publishedAt: firstText(data?.time, data?.create_time, ''),
    likeCount: data?.like ?? data?.statistics?.digg_count ?? data?.aweme_detail?.statistics?.digg_count ?? '',
    sourceUrl: url
  };
}
parseWithJxcxin.providerName = 'Jxcxin';
parseWithJxcxin.providerKey = 'jxcxin';
parseWithJxcxin.isConfigured = () => true;

async function parseWithDevTool(url) {
  const endpoint = new URL('https://www.devtool.top/api/douyin/parse');
  endpoint.searchParams.set('url', url);
  const json = await fetchJson(endpoint);
  const data = json.data;
  const videoUrl = firstUrl(
    data?.nwm_video_url,
    data?.no_watermark_url,
    data?.play,
    data?.video?.play_addr?.url_list,
    data?.aweme_detail?.video?.play_addr?.url_list,
    data?.video?.url,
    data?.video_url,
    data?.url
  );
  if (!videoUrl) {
    throw new Error(json.msg || 'DevTool 未返回视频地址。');
  }
  return {
    provider: 'DevTool',
    title: firstText(data?.title, data?.desc, data?.aweme_detail?.desc, 'douyin-video'),
    author: firstText(data?.author, data?.nickname, data?.author_name, data?.aweme_detail?.author?.nickname, ''),
    avatarUrl: firstUrl(data?.author_avatar, data?.avatar, data?.aweme_detail?.author?.avatar_thumb?.url_list),
    description: firstText(data?.desc, data?.title, ''),
    coverUrl: firstUrl(data?.video?.cover, data?.video?.origin_cover, data?.cover, data?.aweme_detail?.video?.cover?.url_list),
    videoUrl,
    musicUrl: firstUrl(data?.music?.url, data?.music_url, data?.aweme_detail?.music?.play_url?.url_list),
    musicTitle: firstText(data?.music?.title, data?.aweme_detail?.music?.title, ''),
    publishedAt: firstText(data?.create_time, data?.time, ''),
    likeCount: data?.statistics?.digg_count ?? data?.aweme_detail?.statistics?.digg_count ?? '',
    sourceUrl: url
  };
}
parseWithDevTool.providerName = 'DevTool';
parseWithDevTool.providerKey = 'devtool';
parseWithDevTool.isConfigured = () => true;

async function fetchJson(url, options = {}) {
  const { timeoutMs = PROVIDER_REQUEST_TIMEOUT_MS, ...requestOptions } = options;
  const response = await fetchWithTimeout(url, {
    ...requestOptions,
    headers: {
      ...requestHeaders(),
      ...(requestOptions.headers || {})
    }
  }, timeoutMs);
  const text = await response.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`返回内容不是 JSON：${text.slice(0, 80)}`);
  }
  if (!response.ok) {
    throw new Error(json.msg || `HTTP ${response.status}`);
  }
  return json;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

function requestHeaders() {
  return {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36',
    Accept: '*/*'
  };
}

function registerRemoteAsset(job, item, type, remoteUrl, filename, contentType) {
  const id = randomUUID();
  assets.set(id, {
    id,
    jobId: job.id,
    itemId: item.id,
    type,
    remoteUrl,
    filename: safeFilename(filename),
    contentType
  });
  return id;
}

function registerLocalAsset(job, item, type, localPath, filename, contentType) {
  const id = randomUUID();
  assets.set(id, {
    id,
    jobId: job.id,
    itemId: item.id,
    type,
    localPath,
    filename: safeFilename(filename),
    contentType
  });
  return id;
}

async function ensureAssetLocal(asset, jobId, itemId, preferredName = '') {
  if (asset.localPath) {
    await stat(asset.localPath);
    return asset.localPath;
  }
  if (!asset.remoteUrl) {
    throw new Error('文件没有可下载地址。');
  }
  const extension = extname(new URL(asset.remoteUrl).pathname) || extname(asset.filename) || '.bin';
  const name = preferredName || `${asset.type}${extension}`;
  const localPath = join(TMP_ROOT, jobId, itemId, safeFilename(name));
  await downloadRemote(asset.remoteUrl, localPath);
  asset.localPath = localPath;
  return localPath;
}

async function downloadRemote(remoteUrl, targetPath) {
  const response = await fetchWithTimeout(remoteUrl, { headers: requestHeaders() }, 90000);
  if (!response.ok || !response.body) {
    throw new Error(`远程下载失败：HTTP ${response.status}`);
  }
  const length = Number(response.headers.get('content-length') || 0);
  if (length > MAX_DOWNLOAD_BYTES) {
    throw new Error('远程文件超过大小限制。');
  }

  await mkdir(dirname(targetPath), { recursive: true });
  let written = 0;
  const sizeLimiter = new Transform({
    transform(chunk, _encoding, callback) {
      written += chunk.length;
      if (written > MAX_DOWNLOAD_BYTES) {
        callback(new Error('远程文件超过大小限制。'));
        return;
      }
      callback(null, chunk);
    }
  });

  try {
    await pipeline(
      Readable.fromWeb(response.body),
      sizeLimiter,
      createWriteStream(targetPath)
    );
  } catch (error) {
    await rm(targetPath, { force: true });
    throw error;
  }
}

function runFfmpeg(inputPath, outputPath) {
  return new Promise((resolvePromise, rejectPromise) => {
    const ffmpeg = spawn('ffmpeg', [
      '-y',
      '-i',
      inputPath,
      '-vn',
      '-acodec',
      'libmp3lame',
      '-b:a',
      '192k',
      outputPath
    ], { windowsHide: true });

    let stderr = '';
    const timer = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      rejectPromise(new Error('FFmpeg 转码超时。'));
    }, 120000);

    ffmpeg.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 4000) stderr = stderr.slice(-4000);
    });
    ffmpeg.on('error', (error) => {
      clearTimeout(timer);
      rejectPromise(error);
    });
    ffmpeg.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) resolvePromise();
      else rejectPromise(new Error(`FFmpeg 转码失败：${stderr || `exit ${code}`}`));
    });
  });
}

function runFfmpegVideoOptimize(inputPath, outputPath) {
  return new Promise((resolvePromise, rejectPromise) => {
    const ffmpeg = spawn('ffmpeg', [
      '-y',
      '-i',
      inputPath,
      '-map',
      '0:v:0',
      '-map',
      '0:a?',
      '-c:v',
      'libx264',
      '-preset',
      VIDEO_OPTIMIZE_PRESET,
      '-crf',
      String(VIDEO_OPTIMIZE_CRF),
      '-profile:v',
      'high',
      '-pix_fmt',
      'yuv420p',
      '-vf',
      'scale=trunc(iw/2)*2:trunc(ih/2)*2',
      '-c:a',
      'aac',
      '-b:a',
      '192k',
      '-movflags',
      '+faststart',
      outputPath
    ], { windowsHide: true });

    let stderr = '';
    const timer = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      rejectPromise(new Error('高清优化转码超时。'));
    }, 180000);

    ffmpeg.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 4000) stderr = stderr.slice(-4000);
    });
    ffmpeg.on('error', (error) => {
      clearTimeout(timer);
      rejectPromise(error);
    });
    ffmpeg.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) resolvePromise();
      else rejectPromise(new Error(`FFmpeg 高清优化失败：${stderr || `exit ${code}`}`));
    });
  });
}

function runFfmpegCover(inputPath, outputPath) {
  return new Promise((resolvePromise, rejectPromise) => {
    const ffmpeg = spawn('ffmpeg', [
      '-y',
      '-ss',
      '00:00:01',
      '-i',
      inputPath,
      '-vf',
      'scale=540:-2',
      '-frames:v',
      '1',
      outputPath
    ], { windowsHide: true });

    let stderr = '';
    const timer = setTimeout(() => {
      ffmpeg.kill('SIGKILL');
      rejectPromise(new Error('Cover thumbnail generation timed out.'));
    }, 60000);

    ffmpeg.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > 4000) stderr = stderr.slice(-4000);
    });
    ffmpeg.on('error', (error) => {
      clearTimeout(timer);
      rejectPromise(error);
    });
    ffmpeg.on('close', (code) => {
      clearTimeout(timer);
      if (code === 0) resolvePromise();
      else rejectPromise(new Error(`Cover thumbnail generation failed: ${stderr || `exit ${code}`}`));
    });
  });
}

async function rewriteCopy({ sourceText, language = 'zh' }) {
  const apiKey = process.env.REWRITE_API_KEY || process.env.TRANSCRIPTION_API_KEY;
  if (!apiKey) {
    return {
      status: 'fallback',
      provider: 'local',
      text: localRewriteCopy(sourceText, language),
      sourceLength: sourceText.length,
      rewrittenLength: localRewriteCopy(sourceText, language).length,
      reason: language === 'en' ? 'AI rewrite provider is not configured.' : 'AI 改写服务未配置，已生成本地改写建议。'
    };
  }

  const baseUrl = process.env.REWRITE_BASE_URL || 'https://api.siliconflow.cn/v1/chat/completions';
  const model = process.env.REWRITE_MODEL || 'Qwen/Qwen2.5-7B-Instruct';
  const systemPrompt = language === 'en'
    ? 'You are a short-video script rewriting assistant. Rewrite only the exact copied video script provided by the user. Do not rewrite the title, do not infer from the title, do not add unsupported claims, and output only the rewritten copy.'
    : '你是短视频视频文案改写助手。只改写用户复制出来的那段视频文案，不按视频标题或商品标题改写，不新增原文没有的信息，不输出解释，只输出改写后的文案。';
  const userPrompt = language === 'en'
    ? `Copied video script:\n${sourceText}\n\nRewrite ONLY this copied video script. Keep the original meaning and produce polished social video copy.`
    : `复制下来的视频文案：\n${sourceText}\n\n请只改写上面这段“复制文案”的内容，保留原意，改成更顺口、更适合发布的中文短视频文案。`;

  try {
    const response = await fetchWithTimeout(baseUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt }
        ],
        temperature: Number(process.env.REWRITE_TEMPERATURE || 0.75),
        max_tokens: Number(process.env.REWRITE_MAX_TOKENS || 500)
      })
    }, 60000);
    const json = await response.json().catch(() => ({}));
    if (!response.ok) {
      const fallback = localRewriteCopy(sourceText, language);
      return {
        status: 'fallback',
        provider: model,
        text: fallback,
        sourceLength: sourceText.length,
        rewrittenLength: fallback.length,
        reason: rewriteErrorMessage(response.status, json, language)
      };
    }
    const rewritten = cleanRewriteText(
      json.choices?.[0]?.message?.content ||
      json.choices?.[0]?.text ||
      json.output_text ||
      json.text ||
      ''
    );
    if (!rewritten) {
      const fallback = localRewriteCopy(sourceText, language);
      return {
        status: 'fallback',
        provider: model,
        text: fallback,
        sourceLength: sourceText.length,
        rewrittenLength: fallback.length,
        reason: language === 'en' ? 'AI provider returned empty content.' : 'AI 服务未返回有效文案，已生成本地改写建议。'
      };
    }
    return {
      status: 'ok',
      provider: model,
      text: rewritten,
      sourceLength: sourceText.length,
      rewrittenLength: rewritten.length
    };
  } catch (error) {
    const fallback = localRewriteCopy(sourceText, language);
    return {
      status: 'fallback',
      provider: model,
      text: fallback,
      sourceLength: sourceText.length,
      rewrittenLength: fallback.length,
      reason: error.message || (language === 'en' ? 'AI rewrite failed.' : 'AI 改写失败，已生成本地改写建议。')
    };
  }
}

function cleanRewriteText(value) {
  return String(value || '')
    .replace(/^["'“”]+|["'“”]+$/g, '')
    .replace(/^(改写后[:：]|文案[:：]|rewritten copy:)/i, '')
    .trim()
    .slice(0, 3000);
}

function localRewriteCopy(sourceText, language = 'zh') {
  const cleaned = String(sourceText || '')
    .replace(/https?:\/\/\S+/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (language === 'en') {
    return `${cleaned}\n\nA cleaner version for posting: practical, easy to understand, and focused on why this content is useful in everyday use.`;
  }
  const body = cleaned.replace(/[。！？!?]+$/g, '');
  return `${body}。这个内容适合日常直接参考，重点清晰、使用场景明确，想提升效率或整理同类素材时可以先收藏起来。`;
}

function rewriteErrorMessage(status, json = {}, language = 'zh') {
  const providerMessage = json.error?.message || json.message || '';
  if (language === 'en') {
    return `AI rewrite provider returned HTTP ${status}${providerMessage ? `: ${providerMessage}` : ''}. A local rewrite suggestion was generated.`;
  }
  return `AI 改写接口返回 HTTP ${status}${providerMessage ? `：${providerMessage}` : ''}，已生成本地改写建议。`;
}

async function transcribeIfConfigured(mp3Path) {
  const apiKey = process.env.TRANSCRIPTION_API_KEY;
  if (!apiKey) {
    return { status: 'skipped', reason: '转写服务未配置。' };
  }

  const fileStat = await stat(mp3Path);
  const maxBytes = Number(process.env.TRANSCRIPTION_MAX_MB || 25) * 1024 * 1024;
  if (fileStat.size > maxBytes) {
    return { status: 'skipped', reason: '音频超过转写大小限制。' };
  }

  try {
    const form = new FormData();
    const buffer = await readFile(mp3Path);
    form.set('file', new Blob([buffer], { type: 'audio/mpeg' }), 'audio.mp3');
    form.set('model', process.env.TRANSCRIPTION_MODEL || 'whisper-1');
    if (process.env.TRANSCRIPTION_LANGUAGE) {
      form.set('language', process.env.TRANSCRIPTION_LANGUAGE);
    }

    const response = await fetchWithTimeout(process.env.TRANSCRIPTION_BASE_URL || 'https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`
      },
      body: form
    }, 120000);
    const json = await response.json().catch(() => ({}));
    if (!response.ok) {
      return { status: 'failed', reason: transcriptionErrorMessage(response.status, json) };
    }
    const text = json.text || json.data?.text || '';
    if (!text) {
      return { status: 'failed', reason: '转写接口未返回文本。' };
    }
    return { status: 'ok', text };
  } catch (error) {
    return { status: 'failed', reason: error.message || '转写失败。' };
  }
}

function transcriptionErrorMessage(status, json = {}) {
  const providerMessage = json.error?.message || json.message || '';
  if (status === 401 || status === 403) {
    return `转写接口拒绝访问（HTTP ${status}）：请检查 API Key 是否有效、账户权限或免费额度是否已启用。${providerMessage ? `服务返回：${providerMessage}` : ''}`;
  }
  if (status === 429) {
    return `转写接口限流或额度不足（HTTP 429）：请稍后重试或检查账户额度。${providerMessage ? `服务返回：${providerMessage}` : ''}`;
  }
  return providerMessage || `转写接口失败：HTTP ${status}`;
}

async function sendLocalAsset(req, res, asset) {
  const fileStat = await stat(asset.localPath);
  const fileSize = fileStat.size;
  const range = req.headers.range;

  res.setHeader('Accept-Ranges', 'bytes');
  res.setHeader('Content-Type', asset.contentType || 'application/octet-stream');
  res.setHeader('Content-Disposition', contentDisposition(asset.filename, req.query.inline === '1'));

  if (!range) {
    res.setHeader('Content-Length', fileSize);
    await pipeResponse(createReadStream(asset.localPath), res);
    return;
  }

  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match) {
    res.status(416).setHeader('Content-Range', `bytes */${fileSize}`).end();
    return;
  }

  const start = match[1] ? Number(match[1]) : 0;
  const end = match[2] ? Number(match[2]) : fileSize - 1;
  if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= fileSize) {
    res.status(416).setHeader('Content-Range', `bytes */${fileSize}`).end();
    return;
  }

  const safeEnd = Math.min(end, fileSize - 1);
  res.status(206);
  res.setHeader('Content-Range', `bytes ${start}-${safeEnd}/${fileSize}`);
  res.setHeader('Content-Length', safeEnd - start + 1);
  await pipeResponse(createReadStream(asset.localPath, { start, end: safeEnd }), res);
}

async function pipeResponse(readable, res) {
  try {
    await pipeline(readable, res);
  } catch (error) {
    if (!res.destroyed) {
      res.destroy(error);
    }
  }
}

function contentDisposition(filename, inline = false) {
  const fallback = safeFilename(filename || 'download.bin');
  const asciiFallback = asciiFilename(fallback);
  return `${inline ? 'inline' : 'attachment'}; filename="${asciiFallback}"; filename*=UTF-8''${encodeURIComponent(fallback)}`;
}

function archiveFolderName(item, index) {
  const prefix = String(index + 1).padStart(2, '0');
  const title = safeFilename(item.parsed?.title || item.id)
    .replace(/\s+/g, ' ')
    .slice(0, 36)
    .replace(/[. ]+$/g, '');
  return `${prefix}-${title || item.id || 'item'}`;
}

function archiveAssetName(asset, itemIndex, assetIndex) {
  const prefix = String(itemIndex + 1).padStart(2, '0');
  const typeName = {
    video: 'video-hd',
    'source-video': 'video-source',
    cover: 'cover',
    mp3: 'audio',
    bgm: 'bgm',
    transcript: 'transcript'
  }[asset.type] || `file-${assetIndex}`;
  return `${prefix}-${typeName}${archiveAssetExtension(asset)}`;
}

function archiveAssetExtension(asset) {
  const fromName = extname(asset.filename || '');
  if (fromName) return fromName;
  if (asset.contentType?.includes('video')) return '.mp4';
  if (asset.contentType?.includes('audio')) return '.mp3';
  if (asset.contentType?.includes('image')) return '.jpg';
  if (asset.contentType?.includes('text')) return '.txt';
  return '.bin';
}

function asciiFilename(filename) {
  const extension = extname(filename);
  const base = filename
    .replace(extension, '')
    .normalize('NFKD')
    .replace(/[^\x20-\x7E]/g, '')
    .replace(/[^A-Za-z0-9._ -]/g, '-')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 60);
  return `${base || 'download'}${extension || ''}`;
}

function updateJobProgress(job, itemIndex, itemProgress) {
  const total = Math.max(1, job.items.length);
  const safeProgress = clamp(Number(itemProgress), 0, 1);
  if (job.items[itemIndex]) {
    job.items[itemIndex].progress = safeProgress;
  }
  const aggregate = job.items.reduce((sum, item) => sum + clamp(Number(item.progress), 0, 1), 0);
  job.progress = Math.min(99, Math.round((aggregate / total) * 100));
  touch(job);
}

function touch(job) {
  job.updatedAt = Date.now();
}

function toPublicJob(job) {
  return {
    id: job.id,
    status: job.status,
    progress: job.progress,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    quota: job.quota || null,
    guestQuota: job.guestQuota || null,
    quotaNotice: job.quotaNotice || null,
    items: job.items.map((item) => ({
      id: item.id,
      inputUrl: item.inputUrl,
      status: item.status,
      stage: item.stage,
      error: item.error,
      parsed: item.parsed,
      parseMs: item.parseMs || 0,
      cached: Boolean(item.parsed?.cached),
      assets: item.assets,
      scriptText: item.scriptText,
      videoOptimization: item.videoOptimization,
      transcription: item.transcription
    }))
  };
}

function safeFilename(value = 'douyin-file') {
  const cleaned = String(value)
    .replace(/[\\/:*?"<>|]/g, '-')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 90);
  return cleaned || 'douyin-file';
}

function buildScriptText(parsed = {}) {
  return firstText(
    parsed.scriptText,
    parsed.transcriptText,
    parsed.description,
    parsed.title,
    ''
  );
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, value));
}

function parseProviderOrder(value) {
  const names = String(value || '')
    .split(',')
    .map((name) => name.trim().toLowerCase())
    .filter(Boolean);
  return names.length ? names : DEFAULT_DOUYIN_PROVIDERS;
}

function getEnabledProviders() {
  const registry = providerRegistry();
  return DOUYIN_PROVIDER_ORDER
    .map((name) => registry.get(name))
    .filter(Boolean);
}

function getProviderStatus() {
  const registry = providerRegistry();
  return DOUYIN_PROVIDER_ORDER.map((name) => {
    const provider = registry.get(name);
    return {
      name,
      label: provider?.providerName || name,
      available: Boolean(provider),
      configured: Boolean(provider?.isConfigured?.())
    };
  });
}

function providerRegistry() {
  return new Map([
    ['xinyew', parseWithXinyew],
    ['mxin', parseWithMxin],
    ['jxcxin', parseWithJxcxin],
    ['devtool', parseWithDevTool],
    ['tikhub', parseWithTikHub],
    ['makuo', parseWithMakuo],
    ['mujie', parseWithMujie]
  ]);
}

function isSuccessCode(code) {
  return code === undefined || code === null || Number(code) === 0 || Number(code) === 1 || Number(code) === 200;
}

function unwrapTikHubData(json) {
  const data = json.data?.data || json.data?.aweme_detail || json.data || json.aweme_detail || json;
  if (Array.isArray(data)) return data[0] || {};
  return data;
}

function summarizeFailures(messages) {
  const unique = [...new Set(messages.filter(Boolean))];
  return unique.slice(0, 2).join(' / ') || '解析失败';
}

function getCachedParseResult(urls = []) {
  const now = Date.now();
  for (const raw of urls) {
    const key = normalizeParseCacheKey(raw);
    if (!key) continue;
    const cacheItem = parseCache.get(key);
    if (!cacheItem) continue;
    if (cacheItem.expiresAt <= now) {
      parseCache.delete(key);
      continue;
    }
    return clonePlain(cacheItem.data);
  }
  return null;
}

function saveParseResultToCache(urls = [], parsed = {}) {
  if (!PARSE_CACHE_TTL_MS) return;
  const keyList = [...new Set(urls.map(normalizeParseCacheKey).filter(Boolean))];
  if (!keyList.length) return;

  const base = clonePlain(parsed);
  delete base.cached;
  delete base.parseMs;
  const entry = {
    expiresAt: Date.now() + PARSE_CACHE_TTL_MS,
    data: base
  };

  for (const key of keyList) {
    parseCache.set(key, entry);
  }
}

function normalizeParseCacheKey(value) {
  const input = String(value || '').trim();
  if (!input) return '';
  try {
    return new URL(input).toString();
  } catch {
    return input;
  }
}

function clonePlain(value) {
  return JSON.parse(JSON.stringify(value));
}

function firstText(...values) {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value.trim();
    if (typeof value === 'number') return String(value);
  }
  return '';
}

function normalizePublicSiteUrl(value) {
  const fallback = `http://localhost:${PORT}`;
  try {
    const url = new URL(String(value || fallback));
    if (!/^https?:$/i.test(url.protocol)) return fallback;
    return `${url.protocol}//${url.host}`.replace(/\/+$/, '');
  } catch {
    return fallback;
  }
}

function escapeXml(value) {
  return String(value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function firstUrl(...values) {
  for (const value of values.flat(Infinity)) {
    if (typeof value === 'string' && /^https?:\/\//i.test(value.trim())) return value.trim();
    if (value && typeof value === 'object') {
      const nested = firstUrl(value.url, value.uri, value.url_list, value.urls);
      if (nested) return nested;
    }
  }
  return '';
}

function bestBitRatePlayUrls(bitRates) {
  if (!Array.isArray(bitRates)) return [];
  return [...bitRates]
    .filter((item) => item?.play_addr)
    .sort((left, right) => Number(right?.bit_rate || 0) - Number(left?.bit_rate || 0))
    .flatMap((item) => preferredVideoUrls(item.play_addr?.url_list));
}

function preferredVideoUrls(...values) {
  return collectUrls(...values).sort((left, right) => videoHostScore(left) - videoHostScore(right));
}

function collectUrls(...values) {
  const urls = [];
  const visit = (value) => {
    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }
    if (typeof value === 'string' && /^https?:\/\//i.test(value.trim())) {
      urls.push(value.trim());
      return;
    }
    if (value && typeof value === 'object') {
      visit(value.url);
      visit(value.uri);
      visit(value.url_list);
      visit(value.urls);
    }
  };

  values.forEach(visit);
  return [...new Set(urls)];
}

function videoHostScore(url) {
  try {
    const host = new URL(url).host;
    if (host === 'api-play.amemv.com') return 0;
    if (host === 'api.amemv.com') return 1;
    if (host.endsWith('.amemv.com')) return 2;
    if (host.includes('douyinvod.com')) return 3;
    return 4;
  } catch {
    return 5;
  }
}

async function cleanExpiredJobs() {
  const now = Date.now();
  for (const [jobId, job] of jobs) {
    if (now - job.updatedAt < JOB_TTL_MS) continue;
    jobs.delete(jobId);
    for (const [assetId, asset] of assets) {
      if (asset.jobId === jobId) assets.delete(assetId);
    }
    await rm(join(TMP_ROOT, jobId), { recursive: true, force: true });
  }
}

function cleanExpiredParseCache() {
  const now = Date.now();
  for (const [key, item] of parseCache) {
    if (!item || item.expiresAt <= now) {
      parseCache.delete(key);
    }
  }
}

function cleanExpiredSmsCodes() {
  const now = Date.now();
  for (const [phone, code] of smsCodes) {
    if (code.expiresAt < now) smsCodes.delete(phone);
  }
}
