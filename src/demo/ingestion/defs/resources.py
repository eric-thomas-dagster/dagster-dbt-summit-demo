"""
Register shared resources with the Definitions loader.

`load_from_defs_folder` picks up `@definitions`-decorated functions in Python
modules under defs/, so this single-purpose module wires WarehouseResource in
at the top-level `resources` key. Ingestion component subclasses reference it
via ``warehouse: WarehouseResource`` in their compute signatures.
"""

import dagster as dg

from demo.resources import WarehouseResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            # demo_mode=False → writes go to Snowflake (via write_pandas)
            # instead of a local DuckDB file. Snowflake auth reads from env
            # (SNOWFLAKE_ACCOUNT / _USER / _PAT / _ROLE / _WAREHOUSE /
            # SNOWFLAKE_SUMMIT_DATABASE) — same vars the dbt profiles use.
            "warehouse": WarehouseResource(demo_mode=False),
        }
    )
