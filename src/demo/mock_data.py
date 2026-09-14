"""
Synthetic data generators for demo mode.

Every ingestion demo subclass (Fivetran, dlt) calls one of these to produce a
pandas DataFrame, then writes it to DuckDB via ``WarehouseResource.write_table``.
This is the "specs impersonate the tool; the compute actually writes real rows"
pattern from the Chicago Bulls reference — because real rows exist in DuckDB,
dbt then runs for real end-to-end against ingested data.

All generators are deterministic (Faker seeded once) so the demo produces the
same graph shape on every boot. Row counts are small enough that a full
"Materialize all" tick is instant, but large enough that MRR / churn / LTV
calculations produce plausible-looking numbers.

Keyed by ``feed_name`` in the components; adding a new source is one entry
here + one YAML block in the corresponding ingestion defs.yaml.
"""


import datetime as dt
import random
from typing import Callable

import pandas as pd
from faker import Faker

_fake = Faker()
Faker.seed(42)
random.seed(42)

_NOW = dt.datetime(2026, 8, 1, 12, 0, 0)  # freeze "now" for reproducibility
_START = _NOW - dt.timedelta(days=180)


def _rand_ts_between(start: dt.datetime, end: dt.datetime) -> dt.datetime:
    delta_s = int((end - start).total_seconds())
    return start + dt.timedelta(seconds=random.randint(0, delta_s))


# ---------------------------------------------------------------------------
# Stripe (Fivetran)
# ---------------------------------------------------------------------------

_STRIPE_STATUSES = ["succeeded"] * 18 + ["failed", "pending"]  # ~90% succeeded


def stripe_customers(n: int = 200) -> pd.DataFrame:
    """Stripe Customer records — one row per customer id (`cus_*`)."""
    rows = []
    for i in range(n):
        created = _rand_ts_between(_START, _NOW - dt.timedelta(days=1))
        rows.append(
            {
                "id": f"cus_{i:08d}",
                "email": _fake.unique.email(),
                "name": _fake.name(),
                "created": created,
                "_loaded_at": _NOW,
            }
        )
    return pd.DataFrame(rows)


def stripe_charges(n: int = 1200) -> pd.DataFrame:
    """Stripe Charge events — repeated purchases per customer id."""
    rows = []
    for i in range(n):
        customer_idx = random.randint(0, 199)  # aligns with stripe_customers(n=200)
        amount_usd = random.choice([9.99, 19.99, 29.99, 49.99, 99.99, 149.99, 199.99])
        charged = _rand_ts_between(_START, _NOW)
        rows.append(
            {
                "id": f"ch_{i:010d}",
                "customer": f"cus_{customer_idx:08d}",
                "amount": int(amount_usd * 100),  # Stripe stores cents
                "currency": "usd",
                "status": random.choice(_STRIPE_STATUSES),
                "created": charged,
                "_loaded_at": _NOW,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Product events (dlt)
# ---------------------------------------------------------------------------

_PAGES = [
    "/",
    "/pricing",
    "/features",
    "/docs",
    "/blog/mesh",
    "/blog/lineage",
    "/signup",
    "/app/dashboard",
    "/app/settings",
    "/app/billing",
]

_REFERRERS = [
    "https://google.com",
    "https://google.com",
    "https://twitter.com",
    "https://news.ycombinator.com",
    "https://linkedin.com",
    "",  # direct
    "",
]

_SIGNUP_SOURCES = ["organic", "organic", "paid_google", "referral", "content"]


def event_signups(n: int = 350) -> pd.DataFrame:
    """New-user signup events — the identity anchor joining Stripe to product."""
    rows = []
    for i in range(n):
        signed_up = _rand_ts_between(_START, _NOW - dt.timedelta(hours=1))
        rows.append(
            {
                "user_id": f"usr_{i:08d}",
                "email": _fake.unique.email(),
                "signup_source": random.choice(_SIGNUP_SOURCES),
                "signed_up_at": signed_up,
                "_loaded_at": _NOW,
            }
        )
    return pd.DataFrame(rows)


def event_page_views(n: int = 5000) -> pd.DataFrame:
    """Anonymous + identified page-view events — feeds fct_events (incremental)."""
    rows = []
    for i in range(n):
        user_idx = random.randint(0, 349)  # aligns with event_signups(n=350)
        event_at = _rand_ts_between(_START, _NOW)
        rows.append(
            {
                "user_id": f"usr_{user_idx:08d}",
                "session_id": f"ses_{random.randint(0, 999999):06d}",
                "page_path": random.choice(_PAGES),
                "referrer": random.choice(_REFERRERS),
                "event_at": event_at,
                "_loaded_at": _NOW,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Registry — components look up their generator by `feed_name` (from YAML)
# ---------------------------------------------------------------------------

GENERATORS: dict[str, Callable[[], pd.DataFrame]] = {
    "stripe_customers": lambda: stripe_customers(),
    "stripe_charges": lambda: stripe_charges(),
    "event_signups": lambda: event_signups(),
    "event_page_views": lambda: event_page_views(),
}
