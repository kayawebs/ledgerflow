alter table object_briefs
    drop constraint if exists object_briefs_current_unique;

alter table object_briefs
    add constraint object_briefs_chain_id_address_brief_type_period_start_peri_key
    unique (chain_id, address, brief_type, period_start, period_end, inputs_digest);
