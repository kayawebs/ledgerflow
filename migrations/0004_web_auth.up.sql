create table if not exists app_users (
  user_id uuid primary key,
  email text not null unique,
  email_verified_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists email_verification_codes (
  code_id uuid primary key,
  email text not null,
  purpose text not null,
  code_hash text not null,
  requested_ip text,
  requested_user_agent text,
  created_at timestamptz not null default now(),
  expires_at timestamptz not null,
  consumed_at timestamptz
);

create index if not exists idx_email_verification_codes_lookup
  on email_verification_codes (email, purpose, created_at desc);

create table if not exists app_user_sessions (
  session_id uuid primary key,
  user_id uuid not null references app_users(user_id) on delete cascade,
  token_hash text not null unique,
  user_agent text,
  created_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  expires_at timestamptz not null,
  revoked_at timestamptz
);

create index if not exists idx_app_user_sessions_active
  on app_user_sessions (user_id, expires_at desc);

create table if not exists app_user_clusters (
  user_id uuid not null references app_users(user_id) on delete cascade,
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (user_id, cluster_id)
);

create index if not exists idx_app_user_clusters_cluster
  on app_user_clusters (cluster_id);
