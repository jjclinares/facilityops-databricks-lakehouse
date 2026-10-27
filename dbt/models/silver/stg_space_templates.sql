with source as (
    {{ latest_records('space_templates') }}
)

select
    id as space_template_id,
    {{ proper_case('name') }} as space_template_name,
    module_template_id,
    _ingested_at
from source
