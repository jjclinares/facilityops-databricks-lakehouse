with source as (
    {{ latest_records('countries') }}
)

select
    id as country_id,
    {{ proper_case('name') }} as country_name,
    upper({{ clean_string('iso_code') }}) as country_iso_code,
    created_at,
    _ingested_at
from source
