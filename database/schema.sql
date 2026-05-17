-- ClipFlow production PostgreSQL schema.
-- Run this in the production database before replacing the local JSON store.

create extension if not exists "pgcrypto";

create table if not exists users (
  id uuid primary key default gen_random_uuid(),
  account_type text not null check (account_type in ('email', 'phone')),
  email text unique,
  phone text unique,
  display_name text not null,
  role text not null default 'user' check (role in ('admin', 'user')),
  status text not null default 'active' check (status in ('active', 'disabled')),
  password_hash text,
  plan_id text not null default 'free',
  quota_overrides jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_login_at timestamptz
);

create unique index if not exists one_admin_user
on users ((role))
where role = 'admin';

create table if not exists sessions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references users(id) on delete cascade,
  session_hash text not null unique,
  expires_at timestamptz not null,
  created_at timestamptz not null default now()
);

create table if not exists subscriptions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references users(id) on delete cascade,
  plan_id text not null,
  status text not null default 'active',
  current_period_start timestamptz not null default now(),
  current_period_end timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists payments (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references users(id) on delete cascade,
  subscription_id uuid references subscriptions(id) on delete set null,
  provider text not null,
  provider_order_id text unique,
  plan_id text not null,
  amount numeric(12, 2) not null default 0,
  currency text not null default 'CNY',
  status text not null default 'pending',
  raw_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  paid_at timestamptz
);

create table if not exists jobs (
  id uuid primary key default gen_random_uuid(),
  user_id uuid references users(id) on delete set null,
  status text not null default 'queued',
  progress integer not null default 0,
  url_count integer not null default 0,
  success_count integer not null default 0,
  failed_count integer not null default 0,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists job_items (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references jobs(id) on delete cascade,
  input_url text not null,
  status text not null default 'queued',
  stage text,
  title text,
  author text,
  source_provider text,
  error text,
  parsed_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists assets (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references jobs(id) on delete cascade,
  job_item_id uuid references job_items(id) on delete cascade,
  user_id uuid references users(id) on delete set null,
  kind text not null,
  storage_provider text not null,
  storage_key text not null,
  filename text not null,
  mime_type text not null,
  byte_size bigint,
  expires_at timestamptz,
  created_at timestamptz not null default now()
);

create table if not exists usage_records (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references users(id) on delete cascade,
  job_id uuid references jobs(id) on delete set null,
  kind text not null check (kind in ('extract', 'rewrite', 'transcription')),
  amount numeric(12, 2) not null default 1,
  unit text not null,
  period_key text not null,
  created_at timestamptz not null default now()
);

create index if not exists usage_records_user_period_idx on usage_records(user_id, period_key, kind);
create index if not exists jobs_user_created_idx on jobs(user_id, created_at desc);
create index if not exists assets_job_idx on assets(job_id);
