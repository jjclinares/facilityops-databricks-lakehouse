select
    {{ dbt_utils.generate_surrogate_key(['meters.meter_id']) }} as meter_sk,
    meters.meter_id,
    meters.meter_number,
    meters.meter_type,
    meters.provider_name,
    units.unit_sk,
    clients.client_sk
from {{ ref('stg_meters') }} as meters
left join {{ ref('dim_unit') }} as units
    on meters.unit_id = units.unit_id
left join {{ ref('dim_client') }} as clients
    on meters.client_id = clients.client_id
