alter table pilot_interest_submissions
    drop constraint if exists pilot_interest_contact_length_check;

alter table pilot_interest_submissions
    drop constraint if exists pilot_interest_organization_length_check;

alter table pilot_interest_submissions
    drop column if exists contact;

alter table pilot_interest_submissions
    drop column if exists organization;
