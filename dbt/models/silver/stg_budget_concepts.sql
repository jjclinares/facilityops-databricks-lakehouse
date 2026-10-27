with source as (
    {{ latest_records('budget_concepts') }}
)

select
    id as budget_concept_id,
    budget_subitem_id,
    {{ sentence_case('name') }} as concept_name,
    upper({{ clean_string('accounting_code') }}) as accounting_code,
    _ingested_at
from source
