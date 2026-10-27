{#
    Layer schemas are named <prefix><layer>, e.g. `silver` in prod and
    `dev_jane_silver` in a developer deployment. The prefix comes from the
    `schema_prefix` var (set by the Databricks job) or the DBT_SCHEMA_PREFIX
    environment variable (local development).
#}
{% macro schema_prefix() -%}
    {{ return(var('schema_prefix', env_var('DBT_SCHEMA_PREFIX', '')) or '') }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ schema_prefix() ~ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
