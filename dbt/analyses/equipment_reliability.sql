-- MTTR and service hours lost per equipment system.
select
    elements.system_name,
    count(*) as failures,
    round(avg(failures.failure_hours), 1) as mean_time_to_repair_hours,
    round(sum(failures.unavailable_hours), 1) as service_hours_lost,
    count_if(failures.status <> 'Closed') as open_failures
from {{ ref('fact_element_failures') }} as failures
inner join {{ ref('dim_element') }} as elements
    on failures.element_sk = elements.element_sk
group by all
order by service_hours_lost desc
