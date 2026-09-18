-- Traffic counting for the public site. No raw IP or user agent is stored:
-- `visitor_hash` is a salted digest that rotates daily, which is enough to
-- count returning visitors within a day and useless afterwards.
create table if not exists market.page_views (
    id bigserial primary key,
    occurred_at timestamptz not null default now(),
    path text not null,
    referrer_host text,
    utm_source text,
    country text,
    device text,
    visitor_hash text
);

create index if not exists idx_market_page_views_time on market.page_views (occurred_at desc);
create index if not exists idx_market_page_views_path on market.page_views (path, occurred_at desc);
create index if not exists idx_market_page_views_referrer on market.page_views (referrer_host, occurred_at desc)
    where referrer_host is not null;
