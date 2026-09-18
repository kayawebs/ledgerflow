-- Traffic readout for the public market site.
-- Usage: psql -f scripts/market_traffic.sql
\set days 7

\echo '== views and unique visitors per day =='
select date_trunc('day', occurred_at)::date as day,
       count(*) filter (where device <> 'bot') as views,
       count(distinct visitor_hash) filter (where device <> 'bot') as visitors,
       count(*) filter (where device = 'bot') as bot_views
from market.page_views
where occurred_at > now() - make_interval(days => :days)
group by 1 order by 1 desc;

\echo '== most visited pages (people only) =='
select case
         when path like '/launches/%' then '/launches/:id'
         when path like '/address/%' then '/address/:addr'
         when path like '/clusters/%' then '/clusters/:id'
         else path
       end as page,
       count(*) as views,
       count(distinct visitor_hash) as visitors
from market.page_views
where occurred_at > now() - make_interval(days => :days) and device <> 'bot'
group by 1 order by views desc limit 20;

\echo '== where visitors came from =='
select coalesce(referrer_host, '(direct)') as referrer,
       coalesce(utm_source, '-') as campaign,
       count(*) as views,
       count(distinct visitor_hash) as visitors
from market.page_views
where occurred_at > now() - make_interval(days => :days) and device <> 'bot'
group by 1, 2 order by views desc limit 20;

\echo '== device and country mix =='
select device, coalesce(country, '??') as country, count(*) as views
from market.page_views
where occurred_at > now() - make_interval(days => :days)
group by 1, 2 order by views desc limit 20;

\echo '== link unfurls (a share happened) =='
select date_trunc('day', occurred_at)::date as day, count(*) as unfurls
from market.page_views
where occurred_at > now() - make_interval(days => :days) and device = 'bot'
group by 1 order by 1 desc;
