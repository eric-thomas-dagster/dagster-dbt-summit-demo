"""Kick-off job + schedule for the ingestion location.

`run_all_ingestion` materializes all Fivetran + dlt demo assets
(raw_stripe.*, raw_events.*). The `every_15_min_ingestion` schedule fires
it on a cron so the graph is continuously "running" for demos — dbt marts
downstream have AutomationCondition.eager() (see the dbt defs.yaml
post_processing block), so a fresh ingestion cascades through commerce_core
→ subscriptions_analytics → activation without a second click.

The job's selection uses an explicit asset-key list rather than
`AssetSelection.groups(...)` because cross-location remote-graph merge
attaches dbt's `source_not_null_*` / `source_unique_*` checks to these
raw keys (the checks live in commerce_core). Explicit keys bypass the
group-resolution path when the schedule fires the job. Note:
**"Materialize" clicks in the UI lineage graph still error** —
that's a workspace-level Dagster behavior around cross-location check
merging on UI-driven materializes, not something this file can fix.
Use the job or wait for the schedule tick.
"""

import dagster as dg


@dg.definitions
def jobs() -> dg.Definitions:
    ingestion_job = dg.define_asset_job(
        name="run_all_ingestion",
        selection=[
            "raw_stripe/customers",
            "raw_stripe/charges",
            "raw_events/signups",
            "raw_events/page_views",
        ],
        description=(
            "Materialize all raw Fivetran + dlt ingestion assets. Kick this off "
            "first — commerce_core dbt needs these tables in Snowflake to run."
        ),
    )
    return dg.Definitions(
        jobs=[ingestion_job],
        schedules=[
            dg.ScheduleDefinition(
                name="every_15_min_ingestion",
                cron_schedule="*/15 * * * *",
                job=ingestion_job,
                default_status=dg.DefaultScheduleStatus.RUNNING,
                description=(
                    "Continuously refresh raw ingestion every 15 minutes. Downstream "
                    "dbt marts + activation cascade via AutomationCondition.eager()."
                ),
            )
        ],
    )
