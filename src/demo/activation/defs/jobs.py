"""Kick-off job for activation.

Manual trigger for all Census / Hightouch / PowerBI syncs. In practice, users
won't need this often because every activation asset has
``AutomationCondition.eager()`` — they auto-refresh whenever their upstream
mart advances. This job exists for the case where you want to force a resync
of everything (e.g., dashboard demo after a schema change).
"""

import dagster as dg


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            dg.define_asset_job(
                name="refresh_all_activation",
                # .without_checks() strips remote-graph asset-check merges
                # (dbt exposure tests attached to these activation asset keys).
                selection=dg.AssetSelection.groups(
                    "census_salesforce",
                    "hightouch_braze",
                    "powerbi_dashboards",
                ).without_checks(),
                description=(
                    "Force-refresh all activation assets — Census (Salesforce), "
                    "Hightouch (Braze), PowerBI dashboards. Not usually needed "
                    "(activation assets auto-refresh via AutomationCondition.eager), "
                    "but handy for a manual re-sync during a demo."
                ),
            )
        ]
    )
