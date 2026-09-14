{{
    config(
        materialized = 'table',
    )
}}

-- Required by the dbt semantic layer (Feature 4). MetricFlow needs a time
-- spine at DAY granularity or smaller to compute time-windowed metrics.
-- The 4-year window (2024-2027) covers plausible demo timeframes without
-- being wastefully large.
with days as (
    select
        cast(range as date) as date_day
    from range(
        date '2024-01-01',
        date '2028-01-01',
        interval 1 day
    )
)
select * from days
