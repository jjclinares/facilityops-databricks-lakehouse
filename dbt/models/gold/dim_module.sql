select
    {{ dbt_utils.generate_surrogate_key(['modules.unit_module_id']) }} as module_sk,
    modules.unit_module_id,
    templates.module_template_name as module_name,
    modules.cost_center_code,
    modules.cost_center_name,
    units.unit_id,
    units.unit_sk,
    units.unit_name
from {{ ref('stg_unit_modules') }} as modules
left join {{ ref('stg_module_templates') }} as templates
    on modules.module_template_id = templates.module_template_id
left join {{ ref('dim_unit') }} as units
    on modules.unit_id = units.unit_id
