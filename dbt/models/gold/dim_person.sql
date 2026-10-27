select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as person_sk,
    user_id,
    full_name,
    email,
    role,
    user_type,
    is_active,
    is_purchasing_leader,
    to_date(created_at) as created_date
from {{ ref('stg_users') }}
