-- One row per successful Stripe charge = order. Emitted `access: public`
-- so subscriptions_analytics can compute MRR / churn against orders without
-- ever reaching into commerce_core's staging layer.

select
    charge_id                               as order_id,
    stripe_customer_id,
    amount_usd,
    currency,
    charged_at                              as ordered_at,
    date_trunc('day', charged_at)           as ordered_date,
    date_trunc('month', charged_at)         as ordered_month
from {{ ref('stg_stripe_charges') }}
