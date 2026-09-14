"""
Code location 3 of 4 — subscriptions_analytics dbt project (Growth Team).

Consumes commerce_core's `access: public` marts via dbt mesh
(`external_packages: [commerce_core]`) and produces mrr, churn_prediction,
ltv_segments plus the semantic layer.

Exposures declared in exposures.yml use `meta.dagster.asset_key` to align with
the downstream activation asset keys (Census / Hightouch / PowerBI) — since
those live in the separate `activation` location, the exposure ↔ activation
pairs auto-merge at the remote-graph level.
"""

from pathlib import Path

from dagster import definitions
from dagster.components.core.load_defs import load_defs

from demo.subscriptions_analytics import defs as defs_module

_PROJECT_ROOT = Path(__file__).parents[3]


@definitions
def defs():
    return load_defs(defs_root=defs_module, project_root=_PROJECT_ROOT)
