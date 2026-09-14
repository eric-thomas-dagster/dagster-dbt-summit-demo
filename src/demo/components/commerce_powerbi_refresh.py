"""
CommercePowerBIRefresh — PowerBI dashboard-refresh demo subclass.

Subclasses the official ``PowerBIWorkspaceComponent`` from ``dagster-powerbi``
and adds a ``demo_mode`` toggle. In demo mode we skip PowerBI workspace scan
entirely and build a ``@multi_asset`` off YAML-declared ``demo_assets``, then
yield ``MaterializeResult`` with PowerBI-refresh-shaped metadata (refresh_type,
refresh_status, duration_s, dataset_id). In real mode the parent's workspace
scan + semantic model refresh flow drives real materialization.

Story: the finance team consumes the exec revenue dashboard for board-facing
reporting; growth team consumes the cohort dashboard for retention analysis.
Both PowerBI reports auto-refresh in Dagster after their upstream marts are
rebuilt (via ``AutomationCondition.eager()`` — added later in step 10).

YAML schema:

    type: demo.components.CommercePowerBIRefresh

    attributes:
      demo_mode: true
      workspace:
        credentials:
          client_id:     "{{ env.POWERBI_CLIENT_ID }}"
          client_secret: "{{ env.POWERBI_CLIENT_SECRET }}"
          tenant_id:     "{{ env.POWERBI_TENANT_ID }}"
        workspace_id: "demo-workspace-id"
      demo_assets:
        - key: ["powerbi", "exec_revenue_dashboard"]
          kinds: [powerbi]
          deps: [["mrr"], ["fct_orders"]]
          description: "Board-facing revenue dashboard."
"""


import random
from dataclasses import dataclass, field

import dagster as dg
from dagster.components import ComponentLoadContext, ResolvedAssetSpec
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_powerbi import PowerBIWorkspaceComponent


@dataclass
class CommercePowerBIRefresh(PowerBIWorkspaceComponent):
    """PowerBIWorkspaceComponent with a demo mode toggle.

    In demo mode (``demo_mode: true``) the workspace credentials can be
    placeholders — the parent's discovery pipeline is skipped and each spec
    in ``demo_assets`` becomes its own ``@multi_asset`` off the YAML.
    Metadata mirrors what a real PowerBI dataset-refresh would emit.
    """

    demo_mode: bool = True
    demo_assets: list[ResolvedAssetSpec] = field(default_factory=list)

    @property
    def defs_state_config(self) -> DefsStateConfig:
        if self.demo_mode and self.demo_assets:
            key = (
                f"CommercePowerBIRefresh"
                f"[{'_'.join(self.demo_assets[0].key.path)}]"
            )
            return DefsStateConfig.from_args(self.defs_state, default_key=key)
        return super().defs_state_config

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        if not self.demo_assets:
            return dg.Definitions()

        specs = self.demo_assets
        first_key = "_".join(specs[0].key.path)
        func_name = f"powerbi_refresh_{first_key}"

        @dg.multi_asset(name=func_name, specs=specs, can_subset=True)
        def _demo_refresh(context: dg.AssetExecutionContext):
            for spec in specs:
                if spec.key not in context.selected_asset_keys:
                    continue
                duration_s = round(random.uniform(2.0, 8.0), 1)
                context.log.info(
                    "[DEMO] PowerBI refresh %s → %.1fs",
                    spec.key.to_user_string(),
                    duration_s,
                )
                yield dg.MaterializeResult(
                    asset_key=spec.key,
                    metadata={
                        "powerbi_refresh_status": dg.MetadataValue.text(
                            "Completed"
                        ),
                        "refresh_type": dg.MetadataValue.text("full"),
                        "duration_s": dg.MetadataValue.float(duration_s),
                        "dataset_id": dg.MetadataValue.text(
                            f"demo-dataset-{first_key}"
                        ),
                        "demo_mode": dg.MetadataValue.bool(True),
                    },
                )

        return dg.Definitions(assets=[_demo_refresh])
