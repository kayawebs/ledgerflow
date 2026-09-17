with ranked as (
    select brief_id,
           row_number() over (
               partition by chain_id, address, brief_type
               order by generated_at desc, brief_id desc
           ) as position
    from object_briefs
)
delete from object_briefs
where brief_id in (select brief_id from ranked where position > 1);

-- Versions before 0048 could persist aggregate change signals without any
-- transaction references. They are derived data and cannot satisfy the v1
-- evidence contract, so remove them and let the observation worker rebuild
-- them from the deterministic activity rows.
delete from object_change_signals
where signal_type in ('activity_change', 'failure_rate_change', 'gas_cost_change')
  and cardinality(supporting_tx_hashes) = 0;

alter table object_briefs
    drop constraint if exists object_briefs_chain_id_address_brief_type_period_start_peri_key;

alter table object_briefs
    add constraint object_briefs_current_unique
    unique (chain_id, address, brief_type);

comment on constraint object_briefs_current_unique on object_briefs is
    'Keeps one stable, linkable current brief per observed object and period type.';
