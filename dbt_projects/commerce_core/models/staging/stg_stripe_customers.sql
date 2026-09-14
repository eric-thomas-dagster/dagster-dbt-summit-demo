select
    id                                      as stripe_customer_id,
    email,
    name,
    cast(created as timestamp)              as stripe_created_at,
    cast(_loaded_at as timestamp)           as _loaded_at
from {{ source('raw_stripe', 'customers') }}
