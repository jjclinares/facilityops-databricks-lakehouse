select
    {{ dbt_utils.generate_surrogate_key(['clients.client_id']) }} as client_sk,
    clients.client_id,
    clients.client_name,
    clients.commercial_name,
    document_types.document_type_code,
    clients.document_number,
    clients.email,
    clients.phone_number,
    clients.industry,
    cities.city_sk,
    cities.city_name,
    cities.country_name,
    to_date(clients.created_at) as client_since
from {{ ref('stg_clients') }} as clients
left join {{ ref('stg_document_types') }} as document_types
    on clients.document_type_id = document_types.document_type_id
left join {{ ref('dim_city') }} as cities
    on clients.city_id = cities.city_id
