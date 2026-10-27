with source as (
    {{ latest_records('users') }}
)

select
    id as user_id,
    {{ proper_case('first_name') }} as first_name,
    {{ proper_case('last_name') }} as last_name,
    nullif(concat_ws(' ', {{ proper_case('first_name') }}, {{ proper_case('last_name') }}), '') as full_name,
    lower({{ clean_string('email') }}) as email,
    {{ clean_code('role') }} as role_code,
    {{ humanize('role', default='Unknown') }} as role,
    case
        when is_supplier then 'Supplier'
        when is_staff then 'Staff'
        else 'Field operations'
    end as user_type,
    coalesce(is_active, false) as is_active,
    coalesce(is_purchasing_leader, false) as is_purchasing_leader,
    created_at,
    updated_at,
    _ingested_at
from source
