with source as (
    {{ latest_records('module_templates') }}
)

select
    id as module_template_id,
    {{ clean_string('name') }} as module_template_name,
    _ingested_at
from source
