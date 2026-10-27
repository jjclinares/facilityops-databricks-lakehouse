with source as (
    {{ latest_records('activity_plans') }}
)

select
    id as activity_plan_id,
    {{ proper_case('activity_name') }} as activity_name,
    coalesce({{ clean_code('activity_type') }}, 'unspecified') as activity_type_code,
    {{ humanize('activity_type', default='Unspecified') }} as activity_type,
    {{ clean_code('frequency') }} as frequency,
    start_date,
    end_date,
    unit_id,
    element_id,
    owner_id as owner_user_id,
    created_at,
    _ingested_at
from source
