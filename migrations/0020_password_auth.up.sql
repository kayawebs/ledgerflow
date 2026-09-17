alter table app_users
  add column if not exists password_hash text;

alter table email_verification_codes
  add column if not exists pending_password_hash text;
