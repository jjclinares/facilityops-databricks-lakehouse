select
    {{ dbt_utils.generate_surrogate_key(['members.unit_team_member_id']) }} as unit_team_member_sk,
    units.unit_sk,
    people.person_sk,
    to_date(members.created_at) as assigned_date
from {{ ref('stg_unit_team_members') }} as members
inner join {{ ref('dim_unit') }} as units
    on members.unit_id = units.unit_id
inner join {{ ref('dim_person') }} as people
    on members.user_id = people.user_id
