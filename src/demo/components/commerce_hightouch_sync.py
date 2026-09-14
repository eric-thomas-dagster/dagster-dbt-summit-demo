"""
CommerceHightouchSync — Hightouch reverse-ETL demo subclass.

Subclasses the official ``HightouchSyncComponent`` from ``dagster-hightouch``
and adds a ``demo_mode`` toggle. In demo mode we short-circuit the
``ConfigurableHightouchResource.sync_and_poll`` call and emit a plausible
``MaterializeResult`` with Hightouch-shaped metadata (sync_id, sync_details,
sync_run_details, failed_rows). In real mode we delegate to the parent's
``build_defs`` and the real Hightouch API is called.

Story for the demo: the growth team pushes ``churn_risk_bucket`` from
``subscriptions_analytics.churn_prediction`` into Braze so lifecycle emails
can target the high-risk cohort. Different tool than Census (Salesforce for
revenue vs Braze for marketing) — deliberate to show Dagster orchestrating
BOTH reverse-ETL vendors in one graph, not just one.

YAML schema:

    type: demo.components.CommerceHightouchSync

    attributes:
      demo_mode: true
      sync_id: "ht-sync-churn-001"
      asset:
        key: ["braze", "churn_risk_audience"]
        group_name: hightouch_braze
        kinds: [hightouch, braze]
        deps: [["churn_prediction"]]      # subscriptions_analytics.churn_prediction
        description: "Push churn_risk_bucket to Braze retention audiences."
"""


import random

import dagster as dg
from dagster.components import ComponentLoadContext
from dagster_hightouch import HightouchSyncComponent


class CommerceHightouchSync(HightouchSyncComponent):
    """HightouchSyncComponent with a demo mode toggle.

    In demo mode (``demo_mode: true``) the sync_id can be a placeholder and no
    Hightouch / Braze credentials are needed. Metadata mirrors what a real
    ``sync_and_poll`` completion emits, so the Dagster UI event log renders
    the same shape as a real sync.
    """

    demo_mode: bool = True

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        asset = self.asset
        sync_id = self.sync_id
        asset_key_str = "_".join(asset.key.path)
        func_name = f"hightouch_sync_{asset_key_str}"

        @dg.multi_asset(name=func_name, specs=[asset])
        def _demo_sync(context: dg.AssetExecutionContext):
            rows = random.randint(100, 2_000)
            context.log.info(
                "[DEMO] Hightouch sync %s → %d records → %s",
                sync_id,
                rows,
                asset.key.to_user_string(),
            )
            yield dg.MaterializeResult(
                asset_key=asset.key,
                metadata={
                    "sync_id": dg.MetadataValue.text(sync_id),
                    "sync_details": dg.MetadataValue.json(
                        {"id": sync_id, "status": "SUCCESS"}
                    ),
                    "sync_run_details": dg.MetadataValue.json(
                        {
                            "completionRatio": 1.0,
                            "querySize": rows,
                            "failedRows": {},
                        }
                    ),
                    "total_failed_rows": dg.MetadataValue.int(0),
                    "failed_adds": dg.MetadataValue.int(0),
                    "failed_changes": dg.MetadataValue.int(0),
                    "failed_removes": dg.MetadataValue.int(0),
                    "destination_details": dg.MetadataValue.json({}),
                    "query_size": dg.MetadataValue.int(rows),
                    "completion_ratio": dg.MetadataValue.float(1.0),
                    "failed_rows": dg.MetadataValue.int(0),
                    "demo_mode": dg.MetadataValue.bool(True),
                },
            )

        return dg.Definitions(assets=[_demo_sync])
