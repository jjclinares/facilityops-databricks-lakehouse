with source as (
    {{ latest_records('systems') }}
)

select
    id as system_id,
    {{ proper_case('name') }} as system_name,
    _ingested_at
from source
