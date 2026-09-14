# dagster-dbt-summit-demo

A Dagster + dbt end-to-end demo built for dbt Summit 2026, showcasing the [`dagster-dbt`](https://github.com/dagster-io/dagster) `DbtProjectComponent` on Eric's `et/dbt-public-helper-parity` feature branch — sources with auto-derived freshness policies, `build_after`-derived freshness on marts, code-version automation, `dbt/state=modified` slim-CI tagging, contract metadata, exposures + semantic layer as first-class assets, cross-project dbt mesh via `external_packages`.

Everything runs on **MotherDuck** (cloud DuckDB, free tier — no credit card). Four Dagster code locations mapped to four data-platform layers with cross-location remote-graph merges lighting up.

## Architecture

Four code locations, one connected asset graph:

| Location | Owner | Contents |
|---|---|---|
| `summit-ingestion` | Platform | Fivetran + dlt demo components — write mock rows to MotherDuck `raw_stripe.*` / `raw_events.*` |
| `summit-commerce-core` | Platform | dbt project — Stripe + events staging + `dim_customers`, `fct_orders`, `fct_events` marts with `access: public` (mesh interface) |
| `summit-subscriptions-analytics` | Growth | dbt project — `mrr`, `ltv_segments`, `churn_prediction`, semantic layer, exposures. Depends on `commerce_core` via `external_packages` |
| `summit-activation` | Growth + Finance | Census + Hightouch + PowerBI demo components — fake syncs downstream of the subs marts |

Cross-location merges (each fires once code locations load):
- **Feature 1a (sources)** — ingestion Fivetran/dlt materializable AssetsDefinitions merge with commerce_core's dbt source specs
- **Feature 5 (mesh)** — subs' `external_packages: [commerce_core]` stubs merge with commerce_core's real public marts
- **Feature 3 (exposures)** — subs exposures with `meta.dagster.asset_key` merge with activation sync assets

## Continuous heartbeat

- **Ingestion has a 15-min schedule** (`every_15_min_ingestion`, `default_status=RUNNING`) → runs `run_all_ingestion` on cron
- **All dbt marts have `AutomationCondition.eager()`** via `post_processing` → refire when upstreams advance
- **Activation assets have `AutomationCondition.eager()`** at the component level → refire when marts advance

Net effect: one 15-minute tick materializes the full graph end-to-end without any clicks.

## Local dev

Requires a checkout of the `et/dbt-public-helper-parity` branch at `/Users/ericthomas/internal/dagster-oss` (the paths in `[tool.uv.sources]`). Edit those paths in `pyproject.toml` if your monorepo lives elsewhere.

```bash
cp .env.example .env      # then paste your MotherDuck token
uv sync
dg dev                    # 4 code locations load
```

## Cloud deploy

The vendored `wheels/` directory contains pre-built wheels for the parity-branch packages (`dagster`, `dagster-dbt`, `dagster-fivetran`, etc.) — needed because Dagster+ serverless can't resolve `[tool.uv.sources]` local paths. `requirements.txt` reads them.

To deploy to Dagster+:

```bash
# 1. Push MotherDuck token to Cloud
dg plus create env MOTHERDUCK_TOKEN --from-local-env --scope full --global -y

# 2. Deploy each of the four locations (Docker layer cache makes 2-4 fast)
for pair in \
  "summit-ingestion:demo.ingestion.definitions" \
  "summit-commerce-core:demo.commerce_core.definitions" \
  "summit-subscriptions-analytics:demo.subscriptions_analytics.definitions" \
  "summit-activation:demo.activation.definitions"; do
  loc="${pair%%:*}"; mod="${pair##*:}"
  .venv/bin/dagster-cloud serverless deploy-docker . \
    --base-image public.ecr.aws/docker/library/python:3.12-slim \
    --location-name "$loc" \
    --module-name "$mod" \
    --working-directory src \
    --deployment prod
done
```

`--working-directory src` is required — `Path(__file__).parents[3]` in each `definitions.py` resolves to `/opt/dagster/app` only when Python imports `demo` from the source tree, not a wheel install.

## Rebuilding the wheels

If the parity branch advances, refresh `wheels/`:

```bash
for pkg in \
  "dagster" \
  "dagster-pipes" \
  "dagster-graphql" \
  "dagster-webserver" \
  "libraries/dagster-shared" \
  "libraries/dagster-dbt" \
  "libraries/dagster-dg-cli" \
  "libraries/dagster-dg-core" \
  "libraries/dagster-cloud-cli" \
  "libraries/dagster-fivetran" \
  "libraries/dagster-dlt" \
  "libraries/dagster-census" \
  "libraries/dagster-hightouch" \
  "libraries/dagster-powerbi" \
  "dagster-cloud"; do
  (cd /Users/ericthomas/internal/dagster-oss/python_modules/$pkg && \
   uv build --wheel --out-dir "$OLDPWD/wheels")
done
```

## Regenerating the cached dbt state

`src/demo/defs/.local_defs_state/DbtProjectComponent__*/project/` holds the compiled `manifest.json` that the component reads at boot. It's checked in so cloud deploys work without a warehouse round-trip. To refresh after editing a dbt model:

```bash
set -a && source .env && set +a
for proj in commerce_core subscriptions_analytics; do
  cd src/demo/defs/.local_defs_state/DbtProjectComponent__${proj}__/project
  ../../../../../../../.venv/bin/dbt parse --profiles-dir .
  cd -
done
```

Any dbt model edit also needs the source file mirrored into the cached copy:

```bash
cp dbt_projects/subscriptions_analytics/models/marts/mrr.sql \
   src/demo/defs/.local_defs_state/DbtProjectComponent__subscriptions_analytics__/project/models/marts/mrr.sql
```

## Known gotchas

- `.dockerignore` must NOT list `target` as a wildcard — that also strips the cached manifest inside `.local_defs_state/…/project/target/manifest.json`
- Dagster's UI "Materialize" button on individual asset tiles may error with `DagsterInvalidSubsetError` for assets that receive cross-location asset checks (dbt source tests attaching to ingestion asset keys). Use the job button or the schedule instead — those route through explicit key selections
- DuckDB's `date_trunc('month', ts)` returns TIMESTAMP; Snowflake returns DATE. Any dbt model with an enforced `DATE` contract needs `::date` cast at the model boundary
