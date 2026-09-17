delete from address_labels
where source = 'asset_role_classifier';

delete from address_label_assertions
where source_key = 'asset_role_classifier'
  and method_version = 'token_metadata_roles_v1';

delete from label_definitions definitions
where definitions.label_key in ('stablecoin', 'wrapped_native')
  and not exists (
      select 1
      from address_label_assertions assertions
      where assertions.label_key = definitions.label_key
  );

delete from intelligence_sources
where source_key = 'asset_role_classifier';
