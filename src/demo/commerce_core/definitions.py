"""
Code location 2 of 4 — commerce_core dbt project (Platform Team).

Owns the canonical dim/fct layer sitting on top of raw_stripe.* and
raw_events.* (which arrive via the `ingestion` location).

The `access: public` marts (dim_customers, fct_orders, fct_events) form the
mesh boundary — the `subscriptions_analytics` location depends on them via
`external_packages: [commerce_core]` (Feature 5). Because that location lives
separately, its Feature-5 stubs auto-merge with THIS location's real
AssetsDefinitions at the remote-graph level.
"""

from pathlib import Path

from dagster import definitions
from dagster.components.core.load_defs import load_defs

from demo.commerce_core import defs as defs_module

_PROJECT_ROOT = Path(__file__).parents[3]


@definitions
def defs():
    return load_defs(defs_root=defs_module, project_root=_PROJECT_ROOT)
