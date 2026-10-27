with source as (
    {{ latest_records('clients') }}
)

select
    id as client_id,
    {{ clean_string('name') }} as client_name,
    {{ proper_case('commercial_name') }} as commercial_name,
    document_type_id,
    upper(replace({{ clean_string('document_number') }}, ' ', '')) as document_number,
    lower({{ clean_string('email') }}) as email,
    {{ clean_string('phone_number') }} as phone_number,
    {{ proper_case('industry') }} as industry,
    city_id,
    created_at,
    updated_at,
    _ingested_at
from source
