with source as (
    {{ latest_records('cities') }}
)

select
    id as city_id,
    {{ proper_case('name') }} as city_name,
    country_id,
    _ingested_at
from source
