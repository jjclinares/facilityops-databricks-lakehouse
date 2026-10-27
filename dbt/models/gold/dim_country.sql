with primary_currency as (
    select
        country_currencies.country_id,
        currencies.currency_code,
        currencies.currency_name
    from {{ ref('stg_country_currencies') }} as country_currencies
    inner join {{ ref('stg_currencies') }} as currencies
        on country_currencies.currency_id = currencies.currency_id
    -- countries can accept several currencies; report in the primary one
    qualify row_number() over (
        partition by country_currencies.country_id
        order by country_currencies.is_primary desc, currencies.currency_code
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['countries.country_id']) }} as country_sk,
    countries.country_id,
    countries.country_name,
    countries.country_iso_code,
    primary_currency.currency_code,
    primary_currency.currency_name
from {{ ref('stg_countries') }} as countries
left join primary_currency
    on countries.country_id = primary_currency.country_id
