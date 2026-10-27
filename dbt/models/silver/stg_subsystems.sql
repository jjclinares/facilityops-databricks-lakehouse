with source as (
    {{ latest_records('subsystems') }}
)

select
    id as subsystem_id,
    {{ clean_string('name') }} as subsystem_name,
    system_id,
    _ingested_at
from source
