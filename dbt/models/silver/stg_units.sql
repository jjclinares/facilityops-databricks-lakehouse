with source as (
    {{ latest_records('units') }}
)

select
    id as unit_id,
    {{ proper_case('name') }} as unit_name,
    {{ humanize('unit_type', default='Unspecified') }} as unit_type,
    contract_id,
    manager_id as manager_user_id,
    city_id,
    {{ clean_string('legal_name') }} as legal_name,
    {{ clean_string('document_number') }} as document_number,
    {{ clean_string('phone') }} as phone,
    lower({{ clean_string('email') }}) as email,
    {{ clean_string('address_line1') }} as address_line1,
    {{ clean_string('address_line2') }} as address_line2,
    -- the source system stores 0 for "unknown"
    nullif(year_built, 0) as year_built,
    cast(nullif(constructed_area, 0) as decimal(12, 2)) as constructed_area_m2,
    cast(nullif(rentable_area, 0) as decimal(12, 2)) as rentable_area_m2,
    cast(nullif(common_area, 0) as decimal(12, 2)) as common_area_m2,
    nullif(lat, 0) as latitude,
    nullif(lon, 0) as longitude,
    coalesce(is_active, false) as is_active,
    created_at,
    updated_at,
    _ingested_at
from source
