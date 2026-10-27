with source as (
    {{ latest_records('contracts') }}
)

select
    id as contract_id,
    client_id,
    {{ clean_string('name') }} as contract_name,
    start_date,
    end_date,
    end_date is null or end_date >= current_date() as is_active,
    created_at,
    _ingested_at
from source
