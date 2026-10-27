with source as (
    {{ latest_records('budget_items') }}
)

select
    id as budget_item_id,
    budget_chapter_id,
    {{ sentence_case('name') }} as item_name,
    sort_order as item_order,
    {%- for month in month_names() %}
    cast(coalesce(planned_{{ month }}, 0) as decimal(18, 2)) as planned_{{ month }},
    cast(coalesce(spent_{{ month }}, 0) as decimal(18, 2)) as spent_{{ month }},
    {%- endfor %}
    _ingested_at
from source
