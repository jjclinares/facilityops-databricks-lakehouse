{#
    Service-desk tickets with SLA compliance. Open tickets that are already
    past their SLA are reported as breached instead of waiting for closure.
#}
{{ config(liquid_clustered_by=['created_date', 'unit_sk']) }}

with tickets as (
    select
        *,
        coalesce(sla_response_hours, {{ var('default_response_sla_hours') }}) as response_sla_hours,
        coalesce(sla_resolution_hours, {{ var('default_resolution_sla_hours') }}) as resolution_sla_hours,
        {{ hours_between('created_at', 'first_response_at') }} as first_response_hours,
        {{ hours_between('created_at', 'coalesce(closed_at, current_timestamp())') }} as resolution_hours,
        {{ hours_between('created_at', 'current_timestamp()') }} as age_hours
    from {{ ref('stg_tickets') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['tickets.ticket_id']) }} as ticket_sk,
    tickets.ticket_id,
    tickets.ticket_code,
    tickets.ticket_category,
    tickets.status,
    typologies.typology_name as typology,
    subtypologies.subtypology_name as subtypology,
    tickets.ticket_comment,
    tickets.created_at,
    to_date(tickets.created_at) as created_date,
    tickets.first_response_at,
    tickets.closed_at,
    to_date(tickets.closed_at) as closed_date,
    tickets.closed_at is not null as is_closed,
    tickets.response_sla_hours,
    tickets.resolution_sla_hours,
    tickets.first_response_hours,
    tickets.resolution_hours,
    case
        when tickets.first_response_at is null
            then if(tickets.age_hours > tickets.response_sla_hours, 'Breached', 'Pending')
        when tickets.first_response_hours <= tickets.response_sla_hours then 'Met'
        else 'Breached'
    end as response_sla_status,
    case
        when tickets.status_code = 'suspended' then 'Suspended'
        when tickets.closed_at is null
            then if(tickets.age_hours > tickets.resolution_sla_hours, 'Breached', 'In progress')
        when tickets.resolution_hours <= tickets.resolution_sla_hours then 'Met'
        else 'Breached'
    end as resolution_sla_status,
    clients.client_sk,
    coalesce(units.unit_sk, spaces.unit_sk) as unit_sk,
    spaces.space_sk
from tickets
left join {{ ref('stg_ticket_typologies') }} as typologies
    on tickets.typology_id = typologies.typology_id
left join {{ ref('stg_ticket_subtypologies') }} as subtypologies
    on tickets.subtypology_id = subtypologies.subtypology_id
left join {{ ref('dim_client') }} as clients
    on tickets.client_id = clients.client_id
left join {{ ref('dim_unit') }} as units
    on tickets.unit_id = units.unit_id
left join {{ ref('dim_space') }} as spaces
    on tickets.space_id = spaces.space_id
