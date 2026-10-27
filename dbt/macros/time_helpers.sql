{% macro month_names() -%}
    {{ return(['january', 'february', 'march', 'april', 'may', 'june', 'july',
               'august', 'september', 'october', 'november', 'december']) }}
{%- endmacro %}

{# 'HH-MM' -> minutes after midnight, e.g. '08-30' -> 510 #}
{% macro hhmm_to_minutes(expression) -%}
    try_cast(split_part({{ expression }}, '-', 1) as int) * 60
        + try_cast(split_part({{ expression }}, '-', 2) as int)
{%- endmacro %}

{# minutes after midnight -> 'HH:MM' #}
{% macro minutes_to_hhmm(expression) -%}
    concat(
        lpad(cast(cast(floor(({{ expression }}) / 60) as int) % 24 as string), 2, '0'),
        ':',
        lpad(cast(cast(({{ expression }}) % 60 as int) as string), 2, '0')
    )
{%- endmacro %}

{# Whole hours between two timestamps, as a decimal with minute precision. #}
{% macro hours_between(start_ts, end_ts) -%}
    cast(timestampdiff(MINUTE, {{ start_ts }}, {{ end_ts }}) / 60.0 as decimal(12, 2))
{%- endmacro %}
