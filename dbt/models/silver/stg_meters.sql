with source as (
    {{ latest_records('meters') }}
)

select
    id as meter_id,
    unit_id,
    client_id,
    upper({{ clean_string('meter_number') }}) as meter_number,
    {{ humanize('meter_type', default='Other') }} as meter_type,
    {{ humanize('provider') }} as provider_name,
    created_at,
    _ingested_at
from source
