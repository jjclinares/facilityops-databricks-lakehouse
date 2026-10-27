with source as (
    {{ latest_records('unit_modules') }}
)

select
    id as unit_module_id,
    unit_id,
    module_template_id,
    upper({{ clean_string('cost_center_id') }}) as cost_center_code,
    {{ clean_string('cost_center_name') }} as cost_center_name,
    _ingested_at
from source
