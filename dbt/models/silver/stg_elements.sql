{#
    Parses the free-text service window of each element:
      availability_days      'mon_fri'      -> [1, 2, 3, 4, 5]   (ISO weekdays)
      availability_schedule  '22-00_06-00'  -> 1320 / 360        (minutes after midnight)
    A window whose end <= start crosses midnight; '00-00_00-00' means 24 h.
    Missing or malformed windows default to 24/7.
#}
with source as (
    {{ latest_records('elements') }}
),

parsed as (
    select
        *,
        array_position(
            array('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'),
            {{ clean_code("split_part(availability_days, '_', 1)") }}
        ) as first_weekday,
        array_position(
            array('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'),
            {{ clean_code("split_part(availability_days, '_', 2)") }}
        ) as last_weekday,
        {{ hhmm_to_minutes("split_part(availability_schedule, '_', 1)") }} as window_start_minute,
        {{ hhmm_to_minutes("split_part(availability_schedule, '_', 2)") }} as window_end_minute
    from source
)

select
    id as element_id,
    unit_module_id,
    space_id,
    subsystem_id,
    {{ proper_case('name') }} as element_name,
    upper({{ clean_string('serial') }}) as serial_number,
    coalesce(quantity, 1) as quantity,
    {{ proper_case('installer_name') }} as installer_name,
    {{ clean_string('capacity_description') }} as capacity_description,
    {{ proper_case('brand') }} as brand,
    {{ proper_case('manufacturer') }} as manufacturer,
    {{ humanize('priority', default='Unassigned') }} as priority,
    coalesce(is_active, false) as is_active,
    case
        when first_weekday > 0 and last_weekday >= first_weekday
            then sequence(cast(first_weekday as int), cast(last_weekday as int))
        else sequence(1, 7)
    end as availability_weekdays,
    coalesce(window_start_minute, 0) as availability_start_minute,
    coalesce(window_end_minute, 0) as availability_end_minute,
    receipt_date,
    installation_date,
    manufacturing_date,
    created_at,
    updated_at,
    _ingested_at
from parsed
