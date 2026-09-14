select
    user_id,
    session_id,
    page_path,
    referrer,
    cast(event_at as timestamp)             as event_at,
    cast(_loaded_at as timestamp)           as _loaded_at
from {{ source('raw_events', 'page_views') }}
