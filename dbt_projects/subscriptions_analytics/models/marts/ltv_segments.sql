-- Customer LTV segmentation. Uses commerce_core.dim_customers
-- (lifetime_revenue_usd is pre-aggregated there) and bucketizes into
-- quartiles for downstream marketing activation. Emitted `access: public`.

with ranked as (
    select
        customer_id,
        email,
        lifetime_orders,
        lifetime_revenue_usd,
        ntile(4) over (order by lifetime_revenue_usd) as revenue_quartile
    from {{ ref('dim_customers') }}
    where lifetime_revenue_usd > 0
)
select
    customer_id,
    email,
    lifetime_orders,
    lifetime_revenue_usd,
    revenue_quartile,
    case revenue_quartile
        when 4 then 'whales'
        when 3 then 'core'
        when 2 then 'mid'
        else        'tail'
    end                                     as ltv_segment
from ranked
