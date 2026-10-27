with source as (
    {{ latest_records('spaces') }}
)

select
    id as space_id,
    unit_module_id,
    space_template_id,
    created_at,
    _ingested_at
from source
