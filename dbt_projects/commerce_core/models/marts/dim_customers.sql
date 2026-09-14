-- Canonical customer dimension — joins Stripe billing identity to product
-- signup identity via email. Emitted as `access: public` so downstream projects
-- (subscriptions_analytics) can `ref('commerce_core', 'dim_customers')` across
-- the mesh boundary.

with stripe as (
    select * from {{ ref('stg_stripe_customers') }}
),
signups as (
    select * from {{ ref('stg_events_signups') }}
),
first_charge as (
    select
        stripe_customer_id,
        min(charged_at) as first_charge_at,
        sum(amount_usd) as lifetime_revenue_usd,
        count(*)        as lifetime_orders
    from {{ ref('stg_stripe_charges') }}
    group by 1
)
select
    coalesce(signups.user_id, 'stripe:' || stripe.stripe_customer_id)
                                            as customer_id,
    stripe.stripe_customer_id,
    coalesce(signups.email, stripe.email)   as email,
    stripe.name,
    signups.signed_up_at                    as first_seen_at,
    first_charge.first_charge_at,
    coalesce(first_charge.lifetime_orders, 0)      as lifetime_orders,
    coalesce(first_charge.lifetime_revenue_usd, 0) as lifetime_revenue_usd
from stripe
left join signups on stripe.email = signups.email
left join first_charge on stripe.stripe_customer_id = first_charge.stripe_customer_id
