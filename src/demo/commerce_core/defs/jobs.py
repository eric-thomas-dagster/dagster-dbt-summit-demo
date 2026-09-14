"""Kick-off jobs for commerce_core.

Two jobs surface in the UI:

  - ``run_all_commerce_core`` — materialize the entire commerce_core dbt project
    (staging + dim/fct marts) in one click.
  - ``run_modified_commerce_core`` — materialize ONLY models tagged
    ``dbt/state=modified`` by Feature 3.6. Live-demo moment: edit a SQL file,
    ``dbt parse`` to refresh the current manifest, then hit this job — you
    see exactly one model rebuild (the one you edited), not all 7.
"""

import dagster as dg


@dg.definitions
def jobs() -> dg.Definitions:
    return dg.Definitions(
        jobs=[
            dg.define_asset_job(
                name="run_all_commerce_core",
                # .without_checks() strips cross-location asset checks
                # (activation exposure merges + downstream subs test checks
                # that attach to commerce_core's public marts).
                selection=dg.AssetSelection.groups("commerce_core").without_checks(),
                description=(
                    "Full commerce_core dbt build — staging + dim/fct marts. Kick "
                    "off after ingestion has landed raw data in Snowflake."
                ),
            ),
            dg.define_asset_job(
                name="run_modified_commerce_core",
                # Feature 3.6 (PR #26391): `dbt/state=modified` tags emitted at
                # spec-load time by comparing per-model checksums against
                # prod_state/manifest.json. Programmatically selectable — no
                # dbt CLI wrapper needed, no state selection duplication.
                selection=dg.AssetSelection.tag("dbt/state", "modified"),
                description=(
                    "Materialize ONLY dbt models tagged dbt/state=modified. Edit a "
                    "SQL file + run `dbt parse` in dbt_projects/commerce_core, then "
                    "kick this to see slim-CI selection in action (Feature 3.6)."
                ),
            ),
        ]
    )
