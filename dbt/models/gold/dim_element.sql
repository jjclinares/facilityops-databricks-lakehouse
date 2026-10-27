with elements as (
    select
        *,
        -- service minutes per day; a window ending before it starts crosses midnight
        case
            when availability_end_minute > availability_start_minute
                then availability_end_minute - availability_start_minute
            else availability_end_minute - availability_start_minute + 1440
        end as daily_service_minutes
    from {{ ref('stg_elements') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['elements.element_id']) }} as element_sk,
    elements.element_id,
    elements.element_name,
    systems.system_name,
    subsystems.subsystem_name,
    elements.serial_number,
    elements.quantity,
    elements.brand,
    elements.manufacturer,
    elements.installer_name,
    elements.capacity_description,
    elements.priority,
    if(elements.is_active, 'Available', 'Out of service') as operational_status,
    elements.availability_weekdays,
    elements.availability_start_minute,
    elements.availability_end_minute,
    concat(
        {{ minutes_to_hhmm('elements.availability_start_minute') }},
        '-',
        {{ minutes_to_hhmm('elements.availability_end_minute') }}
    ) as availability_window,
    cast(size(elements.availability_weekdays) * elements.daily_service_minutes / 60.0 as decimal(6, 2))
        as weekly_service_hours,
    elements.receipt_date,
    elements.installation_date,
    elements.manufacturing_date,
    datediff(current_date(), elements.installation_date) as days_in_service,
    modules.module_sk,
    modules.module_name,
    spaces.space_sk,
    spaces.space_name,
    modules.unit_id,
    modules.unit_sk
from elements
left join {{ ref('stg_subsystems') }} as subsystems
    on elements.subsystem_id = subsystems.subsystem_id
left join {{ ref('stg_systems') }} as systems
    on subsystems.system_id = systems.system_id
left join {{ ref('dim_module') }} as modules
    on elements.unit_module_id = modules.unit_module_id
left join {{ ref('dim_space') }} as spaces
    on elements.space_id = spaces.space_id
