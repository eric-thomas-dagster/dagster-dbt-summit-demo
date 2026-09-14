{{ config(materialized='incremental', unique_key='event_id', on_schema_change='sync_all_columns') }}

-- Product event fact table. Incremental to demonstrate mixed-materialization
-- kinds in the same graph (Feature 7 — `enable_materialization_kinds` renders
-- an `incremental` icon on this asset while dim_customers renders `table`).
-- Emitted `access: public` — downstream growth analytics reads this for
-- funnel/cohort work.

with page_views as (
    select
        user_id,
        session_id,
        page_path,
        referrer,
        event_at,
        _loaded_at,
        'page_view'                         as event_type
    from {{ ref('stg_events_page_views') }}

    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
),
signups as (
    select
        user_id,
        cast(null as varchar)               as session_id,
        cast(null as varchar)               as page_path,
        signup_source                       as referrer,
        signed_up_at                        as event_at,
        _loaded_at,
        'signup'                            as event_type
    from {{ ref('stg_events_signups') }}

    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
)
select
    md5(user_id || event_type || cast(event_at as varchar))
                                            as event_id,
    user_id,
    session_id,
    event_type,
    page_path,
    referrer,
    event_at,
    _loaded_at
from (
    select * from page_views
    union all
    select * from signups
) all_events
