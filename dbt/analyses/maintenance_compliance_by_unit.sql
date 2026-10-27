-- Preventive-maintenance compliance per building, last 90 days.
select
    units.unit_name,
    units.city_name,
    count(*) as activities,
    count_if(activities.status = 'Closed on time') as closed_on_time,
    count_if(activities.status = 'Overdue') as overdue,
    round(100 * try_divide(count_if(activities.status = 'Closed on time'), count(*)), 1) as on_time_pct
from {{ ref('fact_maintenance_activities') }} as activities
inner join {{ ref('dim_unit') }} as units
    on activities.unit_sk = units.unit_sk
where activities.scheduled_date between date_sub(current_date(), 90) and current_date()
group by all
order by on_time_pct
