{#
    Budget amounts arrive in wide format (planned_january ... spent_december).
    This model UNPIVOTs item lines and chapter-level taxes into one long table
    at the grain: budget line x month.
#}
with item_lines as (
    select
        'item' as line_type,
        budget_item_id as line_source_id,
        budget_chapter_id,
        item_name as line_name,
        item_order as line_order,
        {%- for month in month_names() %}
        planned_{{ month }},
        spent_{{ month }}{{ "," if not loop.last }}
        {%- endfor %}
    from {{ ref('stg_budget_items') }}
),

tax_lines as (
    select
        'tax' as line_type,
        budget_chapter_id as line_source_id,
        budget_chapter_id,
        'Taxes (VAT)' as line_name,
        999 as line_order,
        {%- for month in month_names() %}
        planned_tax_{{ month }} as planned_{{ month }},
        spent_tax_{{ month }} as spent_{{ month }}{{ "," if not loop.last }}
        {%- endfor %}
    from {{ ref('stg_budget_chapters') }}
),

wide as (
    select * from item_lines
    union all
    select * from tax_lines
),

unpivoted as (
    select *
    from wide
    unpivot include nulls (
        (planned_amount, spent_amount) for month_number in (
            {%- for month in month_names() %}
            (planned_{{ month }}, spent_{{ month }}) as `{{ loop.index }}`{{ "," if not loop.last }}
            {%- endfor %}
        )
    )
)

select
    {{ dbt_utils.generate_surrogate_key(['line_type', 'line_source_id', 'month_number']) }} as budget_line_month_id,
    line_type,
    line_source_id,
    budget_chapter_id,
    line_name,
    line_order,
    cast(month_number as int) as month_number,
    planned_amount,
    spent_amount
from unpivoted
-- untaxed chapters (e.g. CAPEX) carry no tax line
where not (line_type = 'tax' and planned_amount = 0 and spent_amount = 0)
