# dagster-dbt-summit-demo

A Dagster + dbt end-to-end demo, backed by [MotherDuck](https://motherduck.com/) (free tier — no credit card). Four Dagster code locations map to four data-platform layers, and one cascade wires them together:

**Fivetran + dlt ingest → dbt marts (mesh) → semantic layer + exposures → Census / Hightouch / PowerBI activation.**

Every 15 minutes a schedule fires the ingestion job; downstream dbt marts + activation assets have `AutomationCondition.eager()` set, so the whole graph refreshes end-to-end with zero clicks.

## What's in the demo

| Layer | Owner | Contents |
|---|---|---|
| `summit-ingestion` | Platform | Fivetran + dlt components landing `raw_stripe.*` / `raw_events.*` |
| `summit-commerce-core` | Platform | dbt project — Stripe + events staging + `dim_customers`, `fct_orders`, `fct_events` (`access: public` — the mesh interface) |
| `summit-subscriptions-analytics` | Growth | dbt project — `mrr`, `ltv_segments`, `churn_prediction`, semantic models, exposures. Consumes `commerce_core` via dbt mesh (`external_packages`) |
| `summit-activation` | Growth + Finance | Census + Hightouch + PowerBI components — synced downstream of subs marts |

Because the four sit in **separate code locations**, three Dagster remote-graph merges light up in the UI:

- **Sources merge** — ingestion's Fivetran/dlt assets share asset keys with commerce_core's dbt source specs. Fivetran contributes materialization; dbt contributes freshness policy + column schema.
- **Mesh merge** — subs' `external_packages: [commerce_core]` stubs merge with commerce_core's real public marts.
- **Exposures merge** — subs exposures (with `meta.dagster.asset_key`) merge with activation sync assets.

## dbt features on display

The dbt project layer uses [`dagster-dbt`](https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-dbt)'s `DbtProjectComponent` with every opt-in feature enabled:

- **Source freshness** — auto-derived `FreshnessPolicy` from `sources[*].freshness` blocks, plus a companion sensor that runs `dbt source freshness` on a schedule and emits materializations when warehouse timestamps advance
- **Model freshness** — auto-derived `FreshnessPolicy` from dbt 1.9's `config.freshness.build_after` on select marts
- **Code-version automation** — `AutomationCondition.code_version_changed()` on every model, so a SQL edit triggers only the changed models to rebuild
- **`dbt/state=modified` tagging** — slim-CI selection via `AssetSelection.tag("dbt/state", "modified")`, without needing to wrap the dbt CLI
- **Contract metadata + materialization kinds** — enforced contracts surface as structured spec metadata; incremental / view / table kinds render as UI chips
- **Exposures + semantic layer as first-class AssetSpecs** — exposures show as terminal Dagster nodes; semantic models and metrics show as their own group
- **Cross-project dbt mesh** — `external_packages: [commerce_core]` on the downstream project's component; Dagster generates observable stubs that merge remotely

> A couple of these are still in review upstream ([PR #26391](https://github.com/dagster-io/dagster/pull/26391) for the Core enhancements, [#26393](https://github.com/dagster-io/dagster/pull/26393) for the public-helper surface). The demo runs on that feature-branch build in the meantime — see [Feature-branch dependencies](#feature-branch-dependencies) for the mechanics — and drops back to a stock `uv add dagster-dbt` once merged.

## Running it

```bash
cp .env.example .env       # then paste your MotherDuck token
uv sync                    # installs deps (see Feature-branch note below)
dg dev                     # 4 code locations load at http://localhost:3000
```

MotherDuck free tier gives you 10 GB storage + reasonable compute — plenty for the demo. Grab a read/write token from [motherduck.com](https://motherduck.com) → Settings → Access Tokens.

One-time: create the `dbt_summit` database + four schemas that the demo writes to. Either via the MotherDuck UI, or:

```bash
set -a && source .env && set +a
python -c "
import duckdb
c = duckdb.connect('md:')
c.execute('CREATE DATABASE IF NOT EXISTS dbt_summit')
c.execute('USE dbt_summit')
for s in ['raw_stripe', 'raw_events', 'commerce_core', 'subscriptions_analytics']:
    c.execute(f'CREATE SCHEMA IF NOT EXISTS {s}')
"
```

Then hit `run_all_ingestion` in the Dagster UI (or wait 15 min for the schedule tick). Cascade fires the rest.

## Deploying to Dagster+

If you want it running continuously:

```bash
dg plus create env MOTHERDUCK_TOKEN --from-local-env --scope full --global -y

for pair in \
  "summit-ingestion:demo.ingestion.definitions" \
  "summit-commerce-core:demo.commerce_core.definitions" \
  "summit-subscriptions-analytics:demo.subscriptions_analytics.definitions" \
  "summit-activation:demo.activation.definitions"; do
  loc="${pair%%:*}"; mod="${pair##*:}"
  dagster-cloud serverless deploy-docker . \
    --base-image public.ecr.aws/docker/library/python:3.12-slim \
    --location-name "$loc" \
    --module-name "$mod" \
    --working-directory src \
    --deployment prod
done
```

`--working-directory src` matters — the four `definitions.py` files use `Path(__file__).parents[3]` to locate the repo root, which only resolves correctly when `demo` is imported from the source tree.

## Feature-branch dependencies

Because a handful of the parity features listed above haven't landed on `main` yet, the demo installs `dagster` / `dagster-dbt` / the integration libraries from prebuilt wheels checked into `wheels/` (see `requirements.txt`). Locally, the `[tool.uv.sources]` block in `pyproject.toml` points at editable checkouts of the same code — retarget those paths to wherever you have the source cloned, and `uv sync` picks it up.

Once the upstream PRs merge and a `dagster-dbt` release ships them, both the `wheels/` directory and the `[tool.uv.sources]` block go away, and `uv add dagster-dbt` from PyPI replaces the whole setup.

## Known quirks worth calling out

- **UI "Materialize" on individual asset tiles may error with `DagsterInvalidSubsetError`** when the selected asset receives cross-location asset checks (dbt source tests attaching to ingestion asset keys defined in another location). Use the job button or the schedule instead — those route through explicit key selections that dodge the workspace-level check auto-inclusion.
- **DuckDB `date_trunc` returns TIMESTAMP, not DATE.** Any dbt model with an enforced `DATE` contract on a truncated column needs an explicit `::date` cast at the model boundary (see `mrr.sql`).
- **Refreshing the checked-in dbt manifest** (after any model edit): re-run `dbt parse --profiles-dir .` inside `src/demo/defs/.local_defs_state/DbtProjectComponent__<project>__/project/` and mirror the edited model file into that copy so the container ships the up-to-date SQL + manifest together.
