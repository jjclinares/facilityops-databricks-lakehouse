select
    {{ dbt_utils.generate_surrogate_key(['spaces.space_id']) }} as space_sk,
    spaces.space_id,
    templates.space_template_name as space_name,
    modules.module_sk,
    modules.module_name,
    modules.unit_id,
    modules.unit_sk,
    modules.unit_name
from {{ ref('stg_spaces') }} as spaces
left join {{ ref('stg_space_templates') }} as templates
    on spaces.space_template_id = templates.space_template_id
left join {{ ref('dim_module') }} as modules
    on spaces.unit_module_id = modules.unit_module_id
