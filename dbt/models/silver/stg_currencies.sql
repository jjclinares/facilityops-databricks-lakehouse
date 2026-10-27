with source as (
    {{ latest_records('currencies') }}
)

select
    id as currency_id,
    upper({{ clean_string('iso_code') }}) as currency_code,
    {{ proper_case('name') }} as currency_name,
    _ingested_at
from source
