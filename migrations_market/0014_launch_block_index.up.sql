-- The origin resolver walks launches in block order from a cursor. Without an
-- index on that order every pass scans the whole launch table, which is wasted
-- work on a host that is also running the chain node. The index lives here
-- rather than in the public migrations so older binaries are unaffected.
create index if not exists idx_protocol_token_launches_block
    on public.protocol_token_launches (chain_id, launch_block);
