-- Monthly SLA compliance of the service desk.
select
    date_trunc('MONTH', created_date) as month,
    count(*) as tickets,
    round(100 * try_divide(count_if(response_sla_status = 'Met'), count(*)), 1) as response_sla_met_pct,
    round(100 * try_divide(count_if(resolution_sla_status = 'Met'), count_if(is_closed)), 1) as resolution_sla_met_pct,
    round(percentile(resolution_hours, 0.5), 1) as median_resolution_hours
from {{ ref('fact_service_tickets') }}
group by all
order by month
