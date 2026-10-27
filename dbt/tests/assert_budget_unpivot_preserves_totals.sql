-- The wide -> long UNPIVOT must not create or lose money.
{% set months = month_names() %}

with wide_format as (
    select
        sum({% for month in months %}planned_{{ month }}{{ " + " if not loop.last }}{% endfor %}) as planned,
        sum({% for month in months %}spent_{{ month }}{{ " + " if not loop.last }}{% endfor %}) as spent
    from {{ ref('stg_budget_items') }}
),

long_format as (
    select
        sum(planned_amount) as planned,
        sum(spent_amount) as spent
    from {{ ref('int_budget_lines_monthly') }}
    where line_type = 'item'
)

select
    wide_format.planned as wide_planned,
    long_format.planned as long_planned,
    wide_format.spent as wide_spent,
    long_format.spent as long_spent
from wide_format
cross join long_format
where abs(coalesce(wide_format.planned, 0) - coalesce(long_format.planned, 0)) > 0.01
    or abs(coalesce(wide_format.spent, 0) - coalesce(long_format.spent, 0)) > 0.01
