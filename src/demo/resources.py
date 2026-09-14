"""
Shared resources for the demo.

The ``WarehouseResource`` is the single point of contact between demo-mode
ingestion components (which write mock DataFrames into DuckDB) and the real
dbt projects (which read from those same DuckDB tables). One DuckDB file is
shared across:

  - commerce_core dbt project (via profiles.yml env_var DAGSTER_DEMO_DUCKDB)
  - subscriptions_analytics dbt project (same env var)
  - Every demo-mode ingestion component (via this resource)

so the graph flows end-to-end on ``Materialize all`` even though the SaaS APIs
are never touched.

Flipping ``demo_mode=False`` + supplying Snowflake credentials is the entire
prod migration — the resource keeps the same public API.
"""


import logging
import time
from pathlib import Path

import dagster as dg
import pandas as pd

logger = logging.getLogger(__name__)

# Default DuckDB path — kept alongside the demo repo so `dbt debug` in either
# project reaches the same file with just the env_var fallback. Overridden via
# DAGSTER_DEMO_DUCKDB env var (which is also what the dbt profiles.yml read).
_REPO_ROOT = Path(__file__).parents[2]
_DEFAULT_DUCKDB = str(_REPO_ROOT / "demo_warehouse.duckdb")


class WarehouseResource(dg.ConfigurableResource):
    """Read/write pandas DataFrames against the demo warehouse.

    demo_mode=True  → local DuckDB file, zero credentials, colocated with the repo.
    demo_mode=False → MotherDuck (cloud DuckDB) — shared warehouse state across
                      ingestion + both dbt projects + activation. Free tier, no
                      card, works out of the box with dbt-duckdb via a `md:`
                      connection string.

    Ingestion components call ``write_table`` with a DataFrame from
    ``mock_data.GENERATORS[feed_name]``. Marts read via dbt (no resource
    needed on the read path). Activation demos DON'T write — they just emit
    fake ``MaterializeResult`` metadata.
    """

    demo_mode: bool = True
    duckdb_path: str = _DEFAULT_DUCKDB
    # MotherDuck config — token pulled from MOTHERDUCK_TOKEN env var so
    # both `dg dev` and Cloud (env-vars-scoped) work with the same code.
    # `md_database` is the MotherDuck database name that ingestion writes
    # to and both dbt profiles.yml files read from.
    md_database: str = "dbt_summit"

    def write_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "raw",
    ) -> int:
        """Write ``df`` to ``<schema>.<table_name>``. Returns row count."""
        if self.demo_mode:
            return self._write_duckdb(df, table_name, schema)
        return self._write_motherduck(df, table_name, schema)

    def _write_duckdb(self, df: pd.DataFrame, table_name: str, schema: str) -> int:
        import duckdb

        # Retry on the DuckDB single-writer lock. The dbt run and demo-mode
        # ingestion can race under `Materialize all` — a short backoff is
        # enough because writes are tiny.
        last_exc: Exception | None = None
        for attempt in range(8):
            try:
                con = duckdb.connect(self.duckdb_path)
                try:
                    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    con.register("df_temp", df)
                    con.execute(
                        f"CREATE OR REPLACE TABLE {schema}.{table_name} "
                        f"AS SELECT * FROM df_temp"
                    )
                    rows = con.execute(
                        f"SELECT count(*) FROM {schema}.{table_name}"
                    ).fetchone()[0]
                    logger.info(
                        "[DEMO] Wrote %d rows → DuckDB %s.%s",
                        rows,
                        schema,
                        table_name,
                    )
                    return int(rows)
                finally:
                    con.close()
            except Exception as exc:  # duckdb.IOException on lock contention
                last_exc = exc
                wait = 0.5 * (2**attempt)
                logger.warning(
                    "[DEMO] DuckDB write attempt %d failed (%s), retry in %.1fs",
                    attempt + 1,
                    exc,
                    wait,
                )
                time.sleep(wait)
        raise RuntimeError(
            f"DuckDB write failed after 8 attempts: {last_exc}"
        ) from last_exc

    def _write_motherduck(
        self, df: pd.DataFrame, table_name: str, schema: str
    ) -> int:
        """CREATE OR REPLACE the target in MotherDuck via duckdb.connect.

        MOTHERDUCK_TOKEN env var authenticates — same var dbt-duckdb picks
        up. Schema auto-created if missing.
        """
        import os

        import duckdb

        # duckdb picks up MOTHERDUCK_TOKEN from env automatically.
        if not os.environ.get("MOTHERDUCK_TOKEN"):
            raise RuntimeError(
                "MOTHERDUCK_TOKEN env var missing — set in .env locally or in "
                "Cloud env vars via `dg plus create env MOTHERDUCK_TOKEN`."
            )

        conn_str = f"md:{self.md_database}"
        con = duckdb.connect(conn_str)
        try:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            con.register("df_temp", df)
            con.execute(
                f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df_temp"
            )
            rows = con.execute(
                f"SELECT count(*) FROM {schema}.{table_name}"
            ).fetchone()[0]
            logger.info(
                "[DEMO] Wrote %d rows → MotherDuck %s.%s.%s",
                rows,
                self.md_database,
                schema,
                table_name,
            )
            return int(rows)
        finally:
            con.close()
