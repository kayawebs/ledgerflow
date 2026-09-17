drop table if exists address_observation_windows;

delete from intelligence_sources
where source_key = 'canonical_address_facts'
  and not exists (
      select 1
      from address_label_assertions
      where source_key = 'canonical_address_facts'
  )
  and not exists (
      select 1
      from protocol_deployments
      where source_key = 'canonical_address_facts'
  );
