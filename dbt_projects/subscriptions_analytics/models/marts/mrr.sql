-- Monthly Recurring Revenue, computed against commerce_core.fct_orders
-- across the mesh boundary. dagster-dbt Feature 5 emits fct_orders as a
-- stub AssetSpec in this project's code location and merges with
-- commerce_core's real AssetsDefinition automatically.
--
-- Emitted `access: public` so Census can sync this to Salesforce as
-- account_mrr, and PowerBI's exec dashboard consumes it as the top-line
-- revenue tile.

with monthly as (
    select
        ordered_month,
        stripe_customer_id,
        sum(amount_usd) as monthly_revenue_usd,
        count(*)        as monthly_order_count
    from {{ ref('fct_orders') }}
    group by 1, 2
),
mrr_totals as (
    select
        -- DuckDB / MotherDuck: date_trunc('month', ts) returns TIMESTAMP.
        -- Snowflake returns DATE. Cast explicitly so the enforced contract
        -- (`month: DATE`) holds across adapters.
        ordered_month::date                 as month,
        sum(monthly_revenue_usd)            as mrr_usd,
        count(distinct stripe_customer_id)  as paying_customers,
        avg(monthly_revenue_usd)            as arpu_usd
    from monthly
    group by 1
)
select
    month,
    mrr_usd,
    paying_customers,
    arpu_usd,
    mrr_usd - lag(mrr_usd) over (order by month)
                                            as mrr_delta_usd,
    (mrr_usd / nullif(lag(mrr_usd) over (order by month), 0) - 1) * 100
                                            as mrr_growth_pct
from mrr_totals
