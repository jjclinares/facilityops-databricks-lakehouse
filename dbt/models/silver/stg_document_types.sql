with source as (
    {{ latest_records('document_types') }}
)

select
    id as document_type_id,
    upper({{ clean_string('name') }}) as document_type_code,
    country_id,
    _ingested_at
from source
