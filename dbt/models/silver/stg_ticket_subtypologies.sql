with source as (
    {{ latest_records('ticket_subtypologies') }}
)

select
    id as subtypology_id,
    typology_id,
    {{ sentence_case('name') }} as subtypology_name,
    _ingested_at
from source
