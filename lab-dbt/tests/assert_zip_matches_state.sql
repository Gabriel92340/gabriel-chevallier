{{ config(severity='warn') }}

select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
left join {{ ref('states') }} s on u.state = s.code
where s.code is null
    or try_cast(u.zip_code as integer) is null
    or try_cast(u.zip_code as integer) not between s.zip_min and s.zip_max