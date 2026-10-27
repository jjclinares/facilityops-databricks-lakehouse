with source as (
    {{ latest_records('unit_team_members') }}
)

select
    id as unit_team_member_id,
    unit_id,
    user_id,
    created_at,
    _ingested_at
from source
