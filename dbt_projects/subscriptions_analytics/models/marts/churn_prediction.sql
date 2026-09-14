-- Per-customer churn features. Consumes commerce_core.dim_customers +
-- commerce_core.fct_orders across the mesh boundary. Downstream, Hightouch
-- syncs the churn_risk_score to Braze for a "high-risk retention" audience.
--
-- Emitted `access: public` so Hightouch's activation graph can reference
-- this asset key.

with orders_per_customer as (
    select
        stripe_customer_id,
        max(ordered_at)                     as last_order_at,
        count(*)                            as total_orders,
        avg(amount_usd)                     as avg_order_usd,
        max(ordered_at) - min(ordered_at)   as tenure_interval
    from {{ ref('fct_orders') }}
    group by 1
),
scored as (
    select
        c.customer_id,
        c.email,
        c.stripe_customer_id,
        opc.last_order_at,
        opc.total_orders,
        opc.avg_order_usd,
        date_diff('day', opc.last_order_at, current_timestamp)
                                            as days_since_last_order,
        case
            when opc.last_order_at is null then 0.90
            when date_diff('day', opc.last_order_at, current_timestamp) > 90 then 0.75
            when date_diff('day', opc.last_order_at, current_timestamp) > 60 then 0.45
            when date_diff('day', opc.last_order_at, current_timestamp) > 30 then 0.20
            else 0.05
        end                                 as churn_risk_score
    from {{ ref('dim_customers') }} c
    left join orders_per_customer opc
        on c.stripe_customer_id = opc.stripe_customer_id
)
select
    customer_id,
    email,
    stripe_customer_id,
    last_order_at,
    total_orders,
    avg_order_usd,
    days_since_last_order,
    churn_risk_score,
    case
        when churn_risk_score >= 0.70 then 'high_risk'
        when churn_risk_score >= 0.40 then 'medium_risk'
        else 'low_risk'
    end                                     as churn_risk_bucket
from scored
