"""
CommerceCensusSync — Census reverse-ETL demo subclass.

Subclasses the official ``CensusComponent`` from ``dagster-census`` and adds
a ``demo_mode`` toggle. In demo mode the Census API is never touched — the
subclass builds a single ``@multi_asset`` off the YAML-declared ``demo_asset``
and yields a plausible ``MaterializeResult`` with Census-shaped metadata
(sync_id, records_synced, completion_ratio, destination). In real mode the
parent's ``StateBackedComponent`` machinery drives Census workspace discovery
and ``trigger_sync_and_poll``.

The activation side (Census / Hightouch / PowerBI) does NOT write to the
warehouse — it just fakes the SaaS API call. Real materializations here would
be one-way pushes into external systems (Salesforce, Braze, PowerBI).

YAML schema:

    type: demo.components.CommerceCensusSync

    attributes:
      demo_mode: true
      workspace:
        api_key: "{{ env.CENSUS_API_KEY }}"
      sync_selector:
        by_name:
          - mrr_to_salesforce
      sync_id: "sync-mrr-001"
      demo_asset:
        key: ["salesforce", "account_mrr"]
        group_name: census_salesforce
        kinds: [census, salesforce]
        deps: [["mrr"]]                       # subscriptions_analytics.mrr
        description: "Sync monthly MRR to Salesforce accounts."
"""


import random
from dataclasses import dataclass

import dagster as dg
from dagster.components import ComponentLoadContext, ResolvedAssetSpec
from dagster_census import CensusComponent


@dataclass
class CommerceCensusSync(CensusComponent):
    """CensusComponent with a demo mode toggle.

    In demo mode (``demo_mode: true``) no Census / Salesforce credentials are
    needed. The workspace's ``api_key`` can be a placeholder string. The
    asset materialises with plausible reverse-ETL metadata that mirrors what a
    real Census sync completion emits.
    """

    demo_mode: bool = True
    sync_id: str = ""
    demo_asset: ResolvedAssetSpec | None = None

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        if self.demo_asset is None:
            return dg.Definitions()

        spec = self.demo_asset
        sync_id = self.sync_id
        first_key = "_".join(spec.key.path)
        func_name = f"census_sync_{first_key}"

        @dg.multi_asset(name=func_name, specs=[spec], can_subset=True)
        def _demo_sync(context: dg.AssetExecutionContext):
            # Sync sizes vary run-to-run — random keeps successive
            # materializations visually distinct in the UI event log.
            rows = random.randint(100, 5_000)
            context.log.info(
                "[DEMO] Census sync %s → %d records → %s",
                sync_id,
                rows,
                spec.key.to_user_string(),
            )
            yield dg.MaterializeResult(
                asset_key=spec.key,
                metadata={
                    "sync_id": dg.MetadataValue.text(sync_id),
                    "sync_details": dg.MetadataValue.json(
                        {"id": sync_id, "status": "completed"}
                    ),
                    "sync_run_details": dg.MetadataValue.json(
                        {
                            "completionRatio": 1.0,
                            "querySize": rows,
                            "failedRows": {},
                        }
                    ),
                    "records_synced": dg.MetadataValue.int(rows),
                    "completion_ratio": dg.MetadataValue.float(1.0),
                    "failed_rows": dg.MetadataValue.int(0),
                    "total_failed_rows": dg.MetadataValue.int(0),
                    "demo_mode": dg.MetadataValue.bool(True),
                },
            )

        return dg.Definitions(assets=[_demo_sync])
