-- Year-to-date budget execution per building and chapter type (local currency).
select
    units.unit_name,
    budget.currency_code,
    budget.chapter_type,
    sum(budget.planned_amount) as planned,
    sum(budget.spent_amount) as spent,
    round(100 * try_divide(sum(budget.spent_amount), sum(budget.planned_amount)), 1) as execution_pct
from {{ ref('fact_budget') }} as budget
inner join {{ ref('dim_unit') }} as units
    on budget.unit_sk = units.unit_sk
where budget.budget_year = year(current_date())
    and budget.budget_month < date_trunc('MONTH', current_date())
group by all
order by units.unit_name, min(budget.chapter_type_order)
