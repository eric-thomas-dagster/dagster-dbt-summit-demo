"""
Code location 4 of 4 — Activation (Growth Team + Finance).

Reverse-ETL syncs (Census → Salesforce; Hightouch → Braze) plus BI dashboard
refresh (PowerBI). All materializable via demo-mode subclasses that fake the
SaaS API calls but emit tool-shaped MaterializeResult metadata.

Assets in this location:
  salesforce/account_mrr             (Census sync)
  braze/churn_risk_audience          (Hightouch sync)
  braze/ltv_segments_audience        (Hightouch sync)
  powerbi/exec_revenue_dashboard     (PowerBI refresh)
  powerbi/growth_cohort_dashboard    (PowerBI refresh)

Each key matches an exposure declared in the `subscriptions_analytics`
location via `meta.dagster.asset_key` — they auto-merge across code
locations at the remote-graph level (Feature 3 story).
"""

from pathlib import Path

from dagster import definitions
from dagster.components.core.load_defs import load_defs

from demo.activation import defs as defs_module

_PROJECT_ROOT = Path(__file__).parents[3]


@definitions
def defs():
    return load_defs(defs_root=defs_module, project_root=_PROJECT_ROOT)
