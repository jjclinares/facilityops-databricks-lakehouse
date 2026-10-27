with source as (
    {{ latest_records('tickets') }}
)

select
    id as ticket_id,
    upper({{ clean_string('slug') }}) as ticket_code,
    {{ humanize('category', default='Other') }} as ticket_category,
    {{ clean_code('status') }} as status_code,
    {{ humanize('status', default='Unknown') }} as status,
    {{ clean_string('`comment`') }} as ticket_comment,
    client_id,
    unit_id,
    space_id,
    typology_id,
    subtypology_id,
    sla_response_hours,
    sla_resolution_hours,
    created_at,
    first_response_at,
    closed_at,
    updated_at,
    _ingested_at
from source
