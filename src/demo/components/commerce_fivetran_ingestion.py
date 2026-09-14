"""
CommerceFivetranIngestion — Fivetran ingestion demo subclass.

Subclasses the official ``FivetranAccountComponent`` from ``dagster-fivetran``
and adds a ``demo_mode`` toggle. In demo mode we skip Fivetran workspace
discovery entirely and build a ``@multi_asset`` off the YAML-declared
``demo_assets``, writing DataFrames (from ``demo.mock_data.GENERATORS``) into
the local DuckDB warehouse. In real mode we delegate to
``super().build_defs()`` and the official Fivetran discovery / trigger /
poll behaviour is used.

YAML schema:

    type: demo.components.CommerceFivetranIngestion

    attributes:
      demo_mode: true
      workspace:
        account_id: "{{ env.FIVETRAN_ACCOUNT_ID }}"
        api_key:    "{{ env.FIVETRAN_API_KEY }}"
        api_secret: "{{ env.FIVETRAN_API_SECRET }}"
      connector_selector:
        by_name:
          - stripe_customers
      # Demo-mode fields ↓
      feed_name:        stripe_customers   # keys into demo.mock_data.GENERATORS
      warehouse_table:  customers
      warehouse_schema: raw_stripe
      demo_assets:
        - key: ["raw_stripe", "customers"]
          group_name: fivetran_stripe
          kinds: [fivetran, snowflake]
          description: "Stripe Customer records synced via Fivetran."

Resources required
------------------
    warehouse — required in demo mode; the compute writes mock rows to DuckDB
                via ``warehouse.write_table()``. In real mode the official
                FivetranAccountComponent handles the write via Fivetran's API.
"""


import dagster as dg
from dagster.components import ComponentLoadContext, ResolvedAssetSpec
from dagster.components.utils.defs_state import DefsStateConfig
from dagster_fivetran import FivetranAccountComponent

from demo import mock_data as md
from demo.resources import WarehouseResource

# Map pandas dtype → dbt-flavoured type string for TableSchema metadata. Keeps
# the demo-mode column schema visible in the Dagster UI matching what the real
# Fivetran component would emit.
_DTYPE_MAP: dict[str, str] = {
    "int64": "int",
    "float64": "float",
    "object": "string",
    "bool": "boolean",
    "datetime64[ns]": "timestamp",
}


def _dtype_to_dbt(dtype: str) -> str:
    return _DTYPE_MAP.get(dtype, dtype)


class CommerceFivetranIngestion(FivetranAccountComponent):
    """FivetranAccountComponent with a demo mode toggle.

    In demo mode (``demo_mode: true``) no Fivetran API credentials are used —
    the workspace section can be filled with placeholder strings. The mock
    data generator identified by ``feed_name`` produces a DataFrame that gets
    written to ``<warehouse_schema>.<warehouse_table>`` in DuckDB via the
    shared ``WarehouseResource``. Downstream dbt then reads those real rows.

    In real mode (``demo_mode: true``) the parent class's full behaviour is
    used: workspace discovery, connector sync-and-poll, state caching.
    """

    demo_mode: bool = True
    feed_name: str = ""
    warehouse_table: str = ""
    warehouse_schema: str = "raw"
    demo_assets: list[ResolvedAssetSpec] = []

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Two CommerceFivetranIngestion instances can share the same Fivetran
        # account (or same placeholder demo account) — we differentiate their
        # state keys via the first demo asset's key path so cached state
        # doesn't collide.
        if self.demo_mode and self.demo_assets:
            key = (
                f"CommerceFivetranIngestion"
                f"[{'_'.join(self.demo_assets[0].key.path)}]"
            )
            return DefsStateConfig.from_args(self.defs_state, default_key=key)
        return super().defs_state_config

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        if not self.demo_mode:
            return super().build_defs(context)

        if not self.demo_assets:
            return dg.Definitions()

        mock_gen = md.GENERATORS.get(self.feed_name)
        if mock_gen is None:
            raise ValueError(
                f"No mock generator registered for feed_name='{self.feed_name}'. "
                f"Available: {sorted(md.GENERATORS)}"
            )

        # Capture loop-local names so the closure below doesn't reach into
        # ``self`` at execution time — keeps the compute function stable across
        # component reload.
        specs = self.demo_assets
        warehouse_table = self.warehouse_table
        warehouse_schema = self.warehouse_schema
        feed_name = self.feed_name
        first_key = "_".join(specs[0].key.path)
        func_name = f"fivetran_{first_key}"

        @dg.multi_asset(name=func_name, specs=specs, can_subset=True)
        def _demo_asset(
            context: dg.AssetExecutionContext,
            warehouse: WarehouseResource,
        ):
            df = mock_gen()
            context.log.info(
                "[DEMO] Fivetran %s → %d rows → %s.%s",
                feed_name,
                len(df),
                warehouse_schema,
                warehouse_table,
            )
            rows = warehouse.write_table(
                df, warehouse_table, schema=warehouse_schema
            )
            schema = dg.TableSchema(
                columns=[
                    dg.TableColumn(
                        name=col, type=_dtype_to_dbt(str(df[col].dtype))
                    )
                    for col in df.columns
                ]
            )
            for spec in specs:
                if spec.key in context.selected_asset_keys:
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "row_count": dg.MetadataValue.int(rows),
                            "dagster/column_schema": dg.MetadataValue.table_schema(
                                schema
                            ),
                            # Fivetran-realistic metadata so the UI mirrors a
                            # real materialization emitted by the parent class.
                            "rows_synced": dg.MetadataValue.int(rows),
                            "fivetran_status": dg.MetadataValue.text("succeeded"),
                            "demo_mode": dg.MetadataValue.bool(True),
                        },
                    )

        return dg.Definitions(assets=[_demo_asset])
