alter table email_verification_codes
  drop column if exists pending_password_hash;

alter table app_users
  drop column if exists password_hash;
