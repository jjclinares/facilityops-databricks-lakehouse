{#
    Bronze is append-only: every ingestion run adds a new snapshot of each
    source table. Silver keeps only the most recent version of every record.
#}
{% macro latest_records(table_name, key='id', where=none) -%}
    select *
    from {{ source('facilityops', table_name) }}
    {% if where %}where {{ where }}{% endif %}
    qualify row_number() over (
        partition by {{ key }}
        order by _ingested_at desc, _source_file desc
    ) = 1
{%- endmacro %}
