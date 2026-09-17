-- Correct derived action attribution for addresses that only participated in
-- token flows. The enclosing transaction may be a swap or arbitrage, but that
-- does not prove that a settlement/funding recipient executed the strategy.
update object_activity_transactions
set action_label = 'asset_transfer',
    action_confidence = 0.99,
    updated_at = now()
where from_address is not null
  and lower(from_address) <> address
  and (to_address is null or lower(to_address) <> address)
  and (
    jsonb_array_length(token_inflows) > 0
    or jsonb_array_length(token_outflows) > 0
  )
  and action_label <> 'asset_transfer';

-- Workers rebuild materialized profiles from these rows during their existing
-- classifying stage. Object queue/checkpoint state is deliberately untouched.
