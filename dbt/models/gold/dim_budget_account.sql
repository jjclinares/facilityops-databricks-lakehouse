{# Chart of accounts: concept -> subitem -> item -> chapter -> budget (unit, year). #}
select
    {{ dbt_utils.generate_surrogate_key(['concepts.budget_concept_id']) }} as budget_account_sk,
    concepts.budget_concept_id,
    concepts.concept_name,
    concepts.accounting_code,
    subitems.budget_subitem_id,
    subitems.subitem_name,
    items.budget_item_id,
    items.item_name,
    chapters.budget_chapter_id,
    chapters.chapter_name,
    chapter_types.chapter_type_label as chapter_type,
    chapter_types.chapter_type_order,
    budgets.budget_id,
    budgets.budget_year,
    budgets.budget_status,
    units.unit_sk,
    modules.module_sk
from {{ ref('stg_budget_concepts') }} as concepts
inner join {{ ref('stg_budget_subitems') }} as subitems
    on concepts.budget_subitem_id = subitems.budget_subitem_id
inner join {{ ref('stg_budget_items') }} as items
    on subitems.budget_item_id = items.budget_item_id
inner join {{ ref('stg_budget_chapters') }} as chapters
    on items.budget_chapter_id = chapters.budget_chapter_id
inner join {{ ref('stg_budgets') }} as budgets
    on chapters.budget_id = budgets.budget_id
left join {{ ref('budget_chapter_types') }} as chapter_types
    on chapters.chapter_type = chapter_types.chapter_type
left join {{ ref('dim_unit') }} as units
    on budgets.unit_id = units.unit_id
left join {{ ref('dim_module') }} as modules
    on chapters.unit_module_id = modules.unit_module_id
