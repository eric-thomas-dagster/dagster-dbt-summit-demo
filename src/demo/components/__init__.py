"""Custom Dagster components — every subclass here follows the demo-mode pattern:

  1. Subclass the official component from the corresponding integration library
     (dagster-fivetran, dagster-dlt, dagster-census, dagster-hightouch,
     dagster-powerbi).
  2. Add a class-level ``demo_mode: bool = True`` attribute.
  3. Override ``build_defs`` — real mode delegates to ``super().build_defs()``;
     demo mode skips API discovery, builds a ``@multi_asset(can_subset=True)``
     off YAML-declared ``demo_assets``, and yields ``MaterializeResult`` with
     tool-realistic metadata.

The subclasses share a single ``WarehouseResource`` (see demo.resources) so the
ingestion side actually writes real DataFrame rows into DuckDB. Both dbt
projects then read those rows for real. The graph flows end-to-end without any
SaaS APIs — but the code looks production-real because it's the same
component classes a real deployment would use.
"""

from demo.components.commerce_census_sync import CommerceCensusSync
from demo.components.commerce_dlt_ingestion import CommerceDltIngestion
from demo.components.commerce_fivetran_ingestion import CommerceFivetranIngestion
from demo.components.commerce_hightouch_sync import CommerceHightouchSync
from demo.components.commerce_powerbi_refresh import CommercePowerBIRefresh

__all__ = [
    "CommerceCensusSync",
    "CommerceDltIngestion",
    "CommerceFivetranIngestion",
    "CommerceHightouchSync",
    "CommercePowerBIRefresh",
]
