update intelligence_workspaces
set workspace_type = 'custom'
where workspace_type = 'wallet';

alter table intelligence_workspaces
  drop constraint if exists intelligence_workspaces_workspace_type_check;

alter table intelligence_workspaces
  add constraint intelligence_workspaces_workspace_type_check
  check (workspace_type in ('protocol', 'chain', 'searcher', 'fund_research', 'custom', 'demo'));
