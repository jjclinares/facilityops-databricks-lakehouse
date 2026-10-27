{#
    One row per equipment failure. Besides the raw failure duration, it
    computes `unavailable_hours`: only the time that overlaps the element's
    service window (e.g. Mon-Fri 08:00-18:00) counts as lost service. A failure
    over a weekend on an office-hours chiller costs 0 service hours.
#}
{{ config(liquid_clustered_by=['failure_start_date', 'unit_sk']) }}

with failures as (
    select
        *,
        coalesce(ended_at, current_timestamp()) as effective_end_at
    from {{ ref('stg_element_failures') }}
),

elements as (
    select * from {{ ref('dim_element') }}
),

failure_days as (
    -- one row per day touched by the failure, starting the day before so that
    -- overnight windows (22:00-06:00) opened the previous evening are counted
    select
        failures.element_failure_id,
        failures.started_at,
        failures.effective_end_at,
        elements.availability_weekdays,
        elements.availability_start_minute,
        elements.availability_end_minute,
        explode(
            sequence(date_sub(to_date(failures.started_at), 1), to_date(failures.effective_end_at), interval 1 day)
        ) as service_date
    from failures
    inner join elements
        on failures.element_id = elements.element_id
),

service_windows as (
    select
        *,
        timestampadd(MINUTE, availability_start_minute, cast(service_date as timestamp)) as window_start_at,
        timestampadd(
            MINUTE,
            availability_end_minute + if(availability_end_minute <= availability_start_minute, 1440, 0),
            cast(service_date as timestamp)
        ) as window_end_at
    from failure_days
    where array_contains(availability_weekdays, weekday(service_date) + 1)
),

downtime as (
    select
        element_failure_id,
        sum(
            greatest(
                timestampdiff(MINUTE, greatest(window_start_at, started_at), least(window_end_at, effective_end_at)),
                0
            )
        ) / 60.0 as unavailable_hours
    from service_windows
    group by element_failure_id
)

select
    {{ dbt_utils.generate_surrogate_key(['failures.element_failure_id']) }} as element_failure_sk,
    failures.element_failure_id,
    failures.failure_category,
    failures.description,
    if(failures.needs_corrective_maintenance, 'Corrective', 'Preventive') as maintenance_type,
    case
        when failures.ended_at is not null then 'Closed'
        when failures.parts_delivery_at > current_timestamp() then 'Awaiting parts'
        else 'In repair'
    end as status,
    failures.started_at,
    to_date(failures.started_at) as failure_start_date,
    failures.ended_at,
    failures.parts_delivery_at,
    {{ hours_between('failures.started_at', 'failures.effective_end_at') }} as failure_hours,
    cast(coalesce(downtime.unavailable_hours, 0) as decimal(12, 2)) as unavailable_hours,
    failures.corrective_description,
    failures.closing_description,
    elements.element_sk,
    elements.unit_sk,
    owners.person_sk as owner_person_sk
from failures
left join downtime
    on failures.element_failure_id = downtime.element_failure_id
left join elements
    on failures.element_id = elements.element_id
left join {{ ref('stg_activity_plans') }} as plans
    on failures.activity_plan_id = plans.activity_plan_id
left join {{ ref('dim_person') }} as owners
    on plans.owner_user_id = owners.user_id
