select
    {{ dbt_utils.generate_surrogate_key(['cities.city_id']) }} as city_sk,
    cities.city_id,
    cities.city_name,
    countries.country_sk,
    countries.country_name,
    countries.country_iso_code,
    countries.currency_code
from {{ ref('stg_cities') }} as cities
left join {{ ref('dim_country') }} as countries
    on cities.country_id = countries.country_id
