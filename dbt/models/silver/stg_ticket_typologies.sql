with source as (
    {{ latest_records('ticket_typologies') }}
)

select
    id as typology_id,
    {{ clean_string('name') }} as typology_name,
    _ingested_at
from source
