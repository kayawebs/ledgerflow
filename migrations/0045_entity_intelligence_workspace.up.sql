alter table intelligence_workspaces
  drop constraint if exists intelligence_workspaces_workspace_type_check;

alter table intelligence_workspaces
  add constraint intelligence_workspaces_workspace_type_check
  check (workspace_type in ('protocol', 'chain', 'searcher', 'wallet', 'fund_research', 'custom', 'demo'));

update intelligence_workspaces
set name = 'Base Searcher Intelligence Demo', updated_at = now()
where is_demo
  and name in ('Base Execution Intelligence Demo', 'LedgerFlow Public Base Demo');
