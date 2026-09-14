"""
CommerceDltIngestion — dlt ingestion demo subclass.

Subclasses the official ``DltLoadCollectionComponent`` from ``dagster-dlt``
and adds a ``demo_mode`` toggle. In demo mode we skip the dlt pipeline
entirely and build a ``@multi_asset`` off YAML-declared ``demo_assets``, writing
DataFrames from ``demo.mock_data.GENERATORS`` into the local DuckDB warehouse.
In real mode we delegate to ``super().build_defs()`` and the official dlt
pipeline runs.

The dlt story vs Fivetran: dlt is a Python-native pipeline framework, so the
platform team writes ingestion in code (a ``@dlt.source``) rather than
clicking through a SaaS UI. This subclass makes both stories renderable in the
same Dagster graph — different tools, same asset shape.

YAML schema:

    type: demo.components.CommerceDltIngestion

    attributes:
      demo_mode: true
      loads: []                          # empty in demo mode; declare real
                                         # DltLoadSpecs in prod
      feed_name:        event_signups    # keys into demo.mock_data.GENERATORS
      warehouse_table:  signups
      warehouse_schema: raw_events
      demo_assets:
        - key: ["raw_events", "signups"]
          group_name: dlt_events
          kinds: [dlt, snowflake]
          description: "Signup events via dlt Python pipeline."

Resources required
------------------
    warehouse — required in demo mode (writes rows to DuckDB). In real mode
                the dlt pipeline handles its own destination.
"""


from dataclasses import dataclass, field
from typing import Sequence

import dagster as dg
from dagster.components import ComponentLoadContext, ResolvedAssetSpec
from dagster_dlt import DltLoadCollectionComponent
from dagster_dlt.components.dlt_load_collection.component import DltLoadSpecModel

from demo import mock_data as md
from demo.resources import WarehouseResource

_DTYPE_MAP: dict[str, str] = {
    "int64": "int",
    "float64": "float",
    "object": "string",
    "bool": "boolean",
    "datetime64[ns]": "timestamp",
}


def _dtype_to_dbt(dtype: str) -> str:
    return _DTYPE_MAP.get(dtype, dtype)


@dataclass
class CommerceDltIngestion(DltLoadCollectionComponent):
    """DltLoadCollectionComponent with a demo mode toggle.

    In demo mode (``demo_mode: true``) no dlt sources / pipelines are executed
    — the ``loads`` field can be empty. The mock data generator identified by
    ``feed_name`` produces a DataFrame that gets written to
    ``<warehouse_schema>.<warehouse_table>`` in DuckDB via the shared
    ``WarehouseResource``. Downstream dbt then reads those real rows.

    In real mode (``demo_mode: true``) the parent's full behaviour is used:
    each ``DltLoadSpec`` in ``loads`` becomes a ``@dlt_assets`` op driven by
    the DagsterDltResource.
    """

    # Override parent's required `loads` with a default so demo YAML doesn't
    # need to declare fake dlt sources. Real mode still populates this.
    loads: Sequence[DltLoadSpecModel] = field(default_factory=list)
    demo_mode: bool = True
    feed_name: str = ""
    warehouse_table: str = ""
    warehouse_schema: str = "raw"
    demo_assets: list[ResolvedAssetSpec] = field(default_factory=list)

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

        specs = self.demo_assets
        warehouse_table = self.warehouse_table
        warehouse_schema = self.warehouse_schema
        feed_name = self.feed_name
        first_key = "_".join(specs[0].key.path)
        func_name = f"dlt_{first_key}"

        @dg.multi_asset(name=func_name, specs=specs, can_subset=True)
        def _demo_asset(
            context: dg.AssetExecutionContext,
            warehouse: WarehouseResource,
        ):
            df = mock_gen()
            context.log.info(
                "[DEMO] dlt %s → %d rows → %s.%s",
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
                            # dlt-realistic metadata mirroring what a real
                            # dlt pipeline emits — load_id, load_status, jobs.
                            "load_id": dg.MetadataValue.text(
                                f"demo_{feed_name}_{rows}"
                            ),
                            "load_status": dg.MetadataValue.text("completed"),
                            "jobs": dg.MetadataValue.int(1),
                            "demo_mode": dg.MetadataValue.bool(True),
                        },
                    )

        return dg.Definitions(assets=[_demo_asset])
