{# Trim whitespace and turn empty strings into NULL. #}
{% macro clean_string(column) -%}
    nullif(trim({{ column }}), '')
{%- endmacro %}

{# 'Harbor point TOWER ' -> 'Harbor Point Tower' #}
{% macro proper_case(column) -%}
    initcap({{ clean_string(column) }})
{%- endmacro %}

{# 'WATER LEAK' -> 'Water leak' #}
{% macro sentence_case(column) -%}
    concat(upper(left({{ clean_string(column) }}, 1)), lower(substring({{ clean_string(column) }}, 2)))
{%- endmacro %}

{# Enum codes to labels: 'preventive_maintenance' -> 'Preventive Maintenance' #}
{% macro humanize(column, default=none) -%}
    {%- if default is not none -%}
        coalesce(initcap(replace({{ clean_string(column) }}, '_', ' ')), '{{ default }}')
    {%- else -%}
        initcap(replace({{ clean_string(column) }}, '_', ' '))
    {%- endif -%}
{%- endmacro %}

{# Enum codes normalised for joins and filters: ' Closed' -> 'closed' #}
{% macro clean_code(column) -%}
    lower({{ clean_string(column) }})
{%- endmacro %}
