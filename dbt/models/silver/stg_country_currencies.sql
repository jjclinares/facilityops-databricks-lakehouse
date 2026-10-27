with source as (
    {{ latest_records('country_currencies') }}
)

select
    id as country_currency_id,
    country_id,
    currency_id,
    coalesce(is_primary, false) as is_primary,
    _ingested_at
from source
