drop table if exists pilot_interest_submissions;

alter table app_users
    drop constraint if exists app_users_workspace_plan_check;

alter table app_users
    drop column if exists workspace_plan;
