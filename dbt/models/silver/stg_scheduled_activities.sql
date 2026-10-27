with source as (
    {{ latest_records('scheduled_activities') }}
)

select
    id as scheduled_activity_id,
    activity_plan_id,
    {{ proper_case('activity_name') }} as activity_name,
    {{ clean_string('description') }} as description,
    owner_id as owner_user_id,
    scheduled_at,
    to_date(scheduled_at) as scheduled_date,
    created_at,
    started_at,
    completed_at,
    closed_at,
    archived_at,
    _ingested_at
from source
