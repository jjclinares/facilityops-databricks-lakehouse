with source as (
    {{ latest_records('budget_chapters') }}
)

select
    id as budget_chapter_id,
    budget_id,
    unit_module_id,
    {{ proper_case('name') }} as chapter_name,
    {{ clean_code('chapter_type') }} as chapter_type,
    sort_order as chapter_order,
    {%- for month in month_names() %}
    cast(coalesce(planned_tax_{{ month }}, 0) as decimal(18, 2)) as planned_tax_{{ month }},
    cast(coalesce(spent_tax_{{ month }}, 0) as decimal(18, 2)) as spent_tax_{{ month }},
    {%- endfor %}
    _ingested_at
from source
