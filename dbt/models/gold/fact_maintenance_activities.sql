{#
    One row per maintenance activity occurrence, reconciling what *should* have
    happened (plans expanded with their recurrence rule over a calendar) with
    what technicians actually *registered*:

      planned + registered  -> closed on time / closed late / pending / overdue
      planned, never created -> overdue (or pending if in the future)
      registered off-plan    -> rescheduled or ad-hoc work, still counted

    Recurrence rules live in the `activity_frequencies` seed, so adding a new
    frequency is a CSV change, not a SQL change.
#}
{{ config(liquid_clustered_by=['scheduled_date', 'unit_sk']) }}

{% set window_start = "date_sub(current_date(), " ~ var('activity_lookback_days') ~ ")" %}
{% set window_end = "date_add(current_date(), " ~ var('activity_horizon_days') ~ ")" %}

with calendar as (
    select explode(sequence({{ window_start }}, {{ window_end }}, interval 1 day)) as calendar_date
),

plans as (
    select
        plans.*,
        frequencies.frequency_label,
        frequencies.cadence_unit,
        frequencies.cadence_interval
    from {{ ref('stg_activity_plans') }} as plans
    left join {{ ref('activity_frequencies') }} as frequencies
        on plans.frequency = frequencies.frequency
),

expected as (
    select
        plans.activity_plan_id,
        calendar.calendar_date as scheduled_date
    from plans
    inner join calendar
        on calendar.calendar_date >= plans.start_date
        and (plans.end_date is null or calendar.calendar_date <= plans.end_date)
    where
        (plans.cadence_unit = 'once' and calendar.calendar_date = plans.start_date)
        or (
            plans.cadence_unit = 'day'
            and datediff(calendar.calendar_date, plans.start_date) % nullif(plans.cadence_interval, 0) = 0
        )
        or (
            plans.cadence_unit = 'month'
            and (
                (year(calendar.calendar_date) * 12 + month(calendar.calendar_date))
                - (year(plans.start_date) * 12 + month(plans.start_date))
            ) % nullif(plans.cadence_interval, 0) = 0
            -- a plan starting on the 31st is due on the last day of shorter months
            and day(calendar.calendar_date) = least(day(plans.start_date), day(last_day(calendar.calendar_date)))
        )
),

registered as (
    select *
    from {{ ref('stg_scheduled_activities') }}
    where scheduled_date between {{ window_start }} and {{ window_end }}
),

reconciled as (
    select
        coalesce(registered.activity_plan_id, expected.activity_plan_id) as activity_plan_id,
        coalesce(registered.scheduled_date, expected.scheduled_date) as scheduled_date,
        registered.scheduled_activity_id,
        registered.activity_name,
        registered.description,
        registered.owner_user_id,
        registered.created_at,
        registered.started_at,
        registered.completed_at,
        registered.closed_at,
        registered.archived_at,
        coalesce(registered.closed_at, registered.completed_at) as finished_at,
        expected.activity_plan_id is not null as is_planned_occurrence,
        registered.scheduled_activity_id is not null as is_registered
    from expected
    full outer join registered
        on expected.activity_plan_id = registered.activity_plan_id
        and expected.scheduled_date = registered.scheduled_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'reconciled.activity_plan_id', 'reconciled.scheduled_date', 'reconciled.scheduled_activity_id'
    ]) }} as activity_sk,
    reconciled.activity_plan_id,
    reconciled.scheduled_activity_id,
    coalesce(reconciled.activity_name, plans.activity_name) as activity_name,
    reconciled.description,
    coalesce(plans.activity_type, 'Unspecified') as activity_type,
    plans.frequency_label as frequency,
    reconciled.scheduled_date,
    reconciled.created_at,
    reconciled.started_at,
    reconciled.completed_at,
    reconciled.closed_at,
    case
        when reconciled.finished_at is not null and to_date(reconciled.finished_at) <= reconciled.scheduled_date
            then 'Closed on time'
        when reconciled.finished_at is not null
            then 'Closed late'
        when reconciled.scheduled_date >= current_date()
            then 'Pending'
        else 'Overdue'
    end as status,
    greatest(
        datediff(to_date(coalesce(reconciled.finished_at, current_timestamp())), reconciled.scheduled_date), 0
    ) as days_late,
    reconciled.is_planned_occurrence,
    reconciled.is_registered,
    coalesce(plan_units.unit_sk, elements.unit_sk) as unit_sk,
    elements.element_sk,
    owners.person_sk as owner_person_sk
from reconciled
left join plans
    on reconciled.activity_plan_id = plans.activity_plan_id
left join {{ ref('dim_element') }} as elements
    on plans.element_id = elements.element_id
left join {{ ref('dim_unit') }} as plan_units
    on plans.unit_id = plan_units.unit_id
left join {{ ref('dim_person') }} as owners
    on coalesce(reconciled.owner_user_id, plans.owner_user_id) = owners.user_id
-- archived = cancelled by the planner
where reconciled.archived_at is null
