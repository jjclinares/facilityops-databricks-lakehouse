-- Lost service time can never exceed the failure duration itself.
select
    element_failure_id,
    failure_hours,
    unavailable_hours
from {{ ref('fact_element_failures') }}
where unavailable_hours > failure_hours + 0.02
