-- The win feed asks for closed positions that multiplied, newest first. A
-- partial index keeps that to the few thousand rows that qualify instead of
-- the millions of positions that do not.
create index if not exists idx_market_wallet_positions_wins
    on market.wallet_positions (chain_id, last_trade_at desc)
    where sold_token >= bought_token
      and cost_usd >= 20
      and proceeds_usd >= cost_usd * 2;
