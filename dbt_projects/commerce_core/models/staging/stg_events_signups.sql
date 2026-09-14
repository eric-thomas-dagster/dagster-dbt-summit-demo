select
    user_id,
    email,
    signup_source,
    cast(signed_up_at as timestamp)         as signed_up_at,
    cast(_loaded_at as timestamp)           as _loaded_at
from {{ source('raw_events', 'signups') }}
