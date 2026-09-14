"""Kick-off jobs for subscriptions_analytics.

Mirrors the commerce_core pattern — one job for a full rebuild, one job for
Feature 3.6's ``dbt/state=modified`` slim-CI selection.

Because activation assets have ``AutomationCondition.eager()``, materializing
the marts here will auto-cascade the corresponding Census / Hightouch /
PowerBI refresh — no separate click needed on the activation side.
"""

import dagster as dg


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            dg.define_asset_job(
                name="run_all_subscriptions_analytics",
                # .without_checks() strips cross-location asset checks
                # (activation exposure merges, etc.) that live in other
                # code locations.
                selection=dg.AssetSelection.groups("subscriptions_analytics").without_checks(),
                description=(
                    "Full subscriptions_analytics build — mrr, churn_prediction, "
                    "ltv_segments + semantic layer + saved queries. Requires "
                    "commerce_core dim/fct marts to already exist in Snowflake "
                    "(mesh dependency)."
                ),
            ),
            dg.define_asset_job(
                name="run_modified_subscriptions_analytics",
                selection=dg.AssetSelection.tag("dbt/state", "modified"),
                description=(
                    "Materialize ONLY dbt models tagged dbt/state=modified. Same "
                    "slim-CI story as commerce_core's version (Feature 3.6)."
                ),
            ),
        ]
    )
