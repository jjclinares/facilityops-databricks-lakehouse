{#
    Planned vs. spent at the grain budget line x month, for approved budgets.
    Amounts are in the unit's local currency (see `currency_code`).
#}
{{ config(liquid_clustered_by=['budget_month', 'unit_sk']) }}

select
    lines.budget_line_month_id as budget_line_sk,
    budgets.budget_id,
    units.unit_sk,
    modules.module_sk,
    budgets.budget_year,
    make_date(budgets.budget_year, lines.month_number, 1) as budget_month,
    coalesce(chapter_types.chapter_type_label, 'Other') as chapter_type,
    coalesce(chapter_types.chapter_type_order, 99) as chapter_type_order,
    coalesce(chapter_types.is_expense, true) as is_expense,
    chapters.chapter_name,
    chapters.chapter_order,
    lines.line_type,
    lines.line_name,
    lines.line_order,
    units.currency_code,
    lines.planned_amount,
    lines.spent_amount,
    lines.planned_amount - lines.spent_amount as variance_amount,
    cast(try_divide(lines.spent_amount, lines.planned_amount) as decimal(10, 4)) as execution_ratio
from {{ ref('int_budget_lines_monthly') }} as lines
inner join {{ ref('stg_budget_chapters') }} as chapters
    on lines.budget_chapter_id = chapters.budget_chapter_id
inner join {{ ref('stg_budgets') }} as budgets
    on chapters.budget_id = budgets.budget_id
left join {{ ref('budget_chapter_types') }} as chapter_types
    on chapters.chapter_type = chapter_types.chapter_type
left join {{ ref('dim_unit') }} as units
    on budgets.unit_id = units.unit_id
left join {{ ref('dim_module') }} as modules
    on chapters.unit_module_id = modules.unit_module_id
where budgets.budget_status = 'approved'
