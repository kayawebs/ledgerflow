drop index if exists idx_execution_investigations_workspace;
drop index if exists idx_user_monitors_workspace;
alter table execution_investigations drop column if exists workspace_id;
alter table user_monitors drop column if exists workspace_id;
drop table if exists workspace_notification_destinations;
drop table if exists workspace_monitoring_goals;
drop table if exists workspace_entities;
drop table if exists intelligence_workspaces;
