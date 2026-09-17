alter table pilot_interest_submissions
    add column if not exists organization text;

alter table pilot_interest_submissions
    add column if not exists contact text;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'pilot_interest_organization_length_check'
    ) then
        alter table pilot_interest_submissions
            add constraint pilot_interest_organization_length_check
            check (
                organization is null
                or length(trim(organization)) between 1 and 160
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conname = 'pilot_interest_contact_length_check'
    ) then
        alter table pilot_interest_submissions
            add constraint pilot_interest_contact_length_check
            check (
                contact is null
                or length(trim(contact)) between 1 and 500
            );
    end if;
end
$$;

comment on column pilot_interest_submissions.organization is
    'Organization supplied through the public Pilot request form.';

comment on column pilot_interest_submissions.contact is
    'Preferred follow-up channel supplied through the public Pilot request form; never stores secrets.';
