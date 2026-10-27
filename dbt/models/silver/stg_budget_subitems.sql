with source as (
    {{ latest_records('budget_subitems') }}
)

select
    id as budget_subitem_id,
    budget_item_id,
    {{ sentence_case('name') }} as subitem_name,
    _ingested_at
from source
