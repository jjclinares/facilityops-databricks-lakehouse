with source as (
    {{ latest_records('element_failures') }}
)

select
    id as element_failure_id,
    element_id,
    activity_plan_id,
    {{ humanize('category', default='Uncategorized') }} as failure_category,
    {{ sentence_case('description') }} as description,
    coalesce(needs_corrective_maintenance, false) as needs_corrective_maintenance,
    start_date as started_at,
    end_date as ended_at,
    delivery_date as parts_delivery_at,
    {{ sentence_case('corrective_description') }} as corrective_description,
    {{ sentence_case('closing_description') }} as closing_description,
    created_at,
    _ingested_at
from source
