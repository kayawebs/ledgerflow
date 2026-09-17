-- This migration corrects derived attribution and is intentionally not
-- reversed: the former labels were unsupported projections from enclosing
-- transactions. Raw transaction, receipt, log, trace, and flow evidence is
-- unchanged.
select 1;
