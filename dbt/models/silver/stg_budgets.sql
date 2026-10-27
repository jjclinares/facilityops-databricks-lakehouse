with source as (
    {{ latest_records('budgets') }}
)

select
    id as budget_id,
    unit_id,
    year as budget_year,
    {{ clean_code('status') }} as budget_status,
    created_at,
    _ingested_at
from source
