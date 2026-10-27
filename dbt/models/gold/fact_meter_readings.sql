{#
    Meters report cumulative values; consumption is the delta to the previous
    reading of the same meter. A reading lower than the previous one means the
    meter was replaced or reset, so no consumption is derived for it.
#}
{{ config(liquid_clustered_by=['reading_date', 'meter_sk']) }}

with readings as (
    select
        *,
        lag(reading_value) over (partition by meter_id order by read_at) as previous_reading_value,
        lag(reading_date) over (partition by meter_id order by read_at) as previous_reading_date
    from {{ ref('stg_meter_readings') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['readings.meter_reading_id']) }} as meter_reading_sk,
    readings.meter_reading_id,
    meters.meter_sk,
    meters.unit_sk,
    meters.client_sk,
    readings.reading_date,
    readings.read_at,
    readings.reading_value,
    readings.previous_reading_value,
    case
        when readings.reading_value >= readings.previous_reading_value
            then readings.reading_value - readings.previous_reading_value
    end as consumption,
    datediff(readings.reading_date, readings.previous_reading_date) as days_since_previous_reading,
    coalesce(readings.reading_value < readings.previous_reading_value, false) as is_meter_reset,
    readings.is_estimated,
    readings.observations
from readings
left join {{ ref('dim_meter') }} as meters
    on readings.meter_id = meters.meter_id
