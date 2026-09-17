drop table if exists behavior_assertion_review_events;
drop table if exists behavior_assertion_reviews;
drop table if exists behavior_analysis_runs;

delete from assertion_evidence
where assertion_id in (
    select assertion_id
    from address_label_assertions
    where source_key in ('behavior_window_classifier', 'manual_behavior_review')
);

delete from address_label_assertions
where source_key in ('behavior_window_classifier', 'manual_behavior_review');

delete from label_definitions
where label_key = 'actor:automation_candidate';

delete from intelligence_sources
where source_key in ('behavior_window_classifier', 'manual_behavior_review');
