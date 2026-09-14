select
    id                                      as charge_id,
    customer                                as stripe_customer_id,
    cast(amount as double) / 100.0          as amount_usd,
    currency,
    status,
    cast(created as timestamp)              as charged_at,
    cast(_loaded_at as timestamp)           as _loaded_at
from {{ source('raw_stripe', 'charges') }}
where status = 'succeeded'
