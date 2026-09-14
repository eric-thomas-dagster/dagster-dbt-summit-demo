"""
Code location 1 of 4 — Ingestion (Platform Team).

Loads the Fivetran and dlt demo-mode components + registers the
WarehouseResource that they share to write mock DataFrames into the local
DuckDB warehouse. Downstream code locations (commerce_core, subs, activation)
read from that same DuckDB.

Assets in this location:
  raw_stripe/customers, raw_stripe/charges   (via Fivetran demo subclass)
  raw_events/signups, raw_events/page_views  (via dlt demo subclass)

The dbt `commerce_core` location declares those same AssetKeys as sources
(via `enable_source_assets: true`). Because they live in different code
locations, Dagster's remote-graph resolution auto-merges the pairs: Fivetran /
dlt contributes the materializable AssetsDefinition, dbt contributes the
freshness policy + column schema metadata from sources.yml. One node in the
UI — Feature 1a.
"""

from pathlib import Path

from dagster import definitions
from dagster.components.core.load_defs import load_defs

from demo.ingestion import defs as defs_module

_PROJECT_ROOT = Path(__file__).parents[3]


@definitions
def defs():
    return load_defs(defs_root=defs_module, project_root=_PROJECT_ROOT)
