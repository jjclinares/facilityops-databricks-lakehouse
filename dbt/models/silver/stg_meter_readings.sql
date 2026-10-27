{#
    The largest and fastest-growing table, so it is built incrementally: each
    run only processes bronze rows ingested since the previous run and MERGEs
    them on the reading id. Liquid clustering keeps per-meter scans cheap.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='meter_reading_id',
        liquid_clustered_by=['meter_id', 'reading_date'],
        on_schema_change='append_new_columns'
    )
}}

{% set new_rows_only %}
    _ingested_at > (select coalesce(max(_ingested_at), timestamp'1900-01-01') from {{ this }})
{% endset %}

with source as (
    {{ latest_records('meter_readings', where=new_rows_only if is_incremental() else none) }}
)

select
    id as meter_reading_id,
    meter_id,
    cast(reading as decimal(18, 2)) as reading_value,
    reading_datetime as read_at,
    to_date(reading_datetime) as reading_date,
    {{ clean_string('observations') }} as observations,
    coalesce({{ clean_string('observations') }} ilike 'estimated%', false) as is_estimated,
    recorded_by_id as recorded_by_user_id,
    _ingested_at
from source
