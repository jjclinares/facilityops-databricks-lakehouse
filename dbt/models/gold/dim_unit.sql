select
    {{ dbt_utils.generate_surrogate_key(['units.unit_id']) }} as unit_sk,
    units.unit_id,
    units.unit_name,
    units.unit_type,
    units.legal_name,
    units.document_number,
    units.phone,
    units.email,
    units.address_line1,
    units.address_line2,
    units.year_built,
    units.constructed_area_m2,
    units.rentable_area_m2,
    units.common_area_m2,
    units.latitude,
    units.longitude,
    units.is_active,
    contracts.contract_id,
    contracts.contract_name,
    clients.client_sk,
    clients.client_name,
    cities.city_sk,
    cities.city_name,
    cities.country_name,
    cities.currency_code,
    managers.person_sk as manager_person_sk,
    managers.full_name as manager_name
from {{ ref('stg_units') }} as units
left join {{ ref('stg_contracts') }} as contracts
    on units.contract_id = contracts.contract_id
left join {{ ref('dim_client') }} as clients
    on contracts.client_id = clients.client_id
left join {{ ref('dim_city') }} as cities
    on units.city_id = cities.city_id
left join {{ ref('dim_person') }} as managers
    on units.manager_user_id = managers.user_id
