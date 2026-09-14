# Integration Metadata: Default Identification + Insights Refactor

**Author:** Eric Thomas
**Status:** Draft for engineering review
**Date:** 2026-09-01

## Summary

Two related proposals in one doc.

**Layer 1 — Default workload identification.** Every Dagster warehouse/compute integration ships identification metadata (query tags, job labels, application names) by default. On automatically, no SDK, no config, no Insights add-on.

**Layer 2 — Insights refactor.** The Insights framework today is two hardcoded pipelines (Snowflake, BigQuery) sharing a UI, not a framework. Collapse it onto four cost-attribution primitives, replace hardcoded per-provider code with backend + UI registries, expose an asset-centric API. This is the opt-in layer that provides per-asset customer-visible cost data on top of the default identification.

**The layering.** Every customer gets identification (Layer 1) automatically. Customers who want per-asset cost attribution add Insights (Layer 2). Layer 2 uses the same channels as Layer 1 with richer payload — strict superset, not a parallel system.

**Ask:** approval on both layers as a coordinated design, plus ownership decisions for each.

## What we already do (Layer 1 is not new)

This proposal isn't inventing a new mechanism — Dagster already ships partner attribution in three integrations. Layer 1 generalizes the same pattern uniformly.

- **`dagster-snowflake`** — Registers as a Snowflake Partner Application: `application = "DagsterLabs_Dagster"` on every connection. Every query through `SnowflakeResource.get_connection()` shows up in Snowflake's partner analytics as Dagster.
  *Refs:* `dagster-snowflake/constants.py:6`, `dagster-snowflake/resources.py:361`
- **`dagster-databricks`** — Sets `product = "dagster-databricks"` on the Databricks SDK `WorkspaceClient`, which populates the `user_agent` on every Databricks API call.
  *Refs:* `dagster-databricks/databricks.py:90` (plus 6 call sites at 101, 110, 118, 128, 139, 147)
- **`dagster-dbt-cloud`** — Tags every dbt Cloud job trigger with a `cause` field (defaults to `"Triggered via Dagster"` in v1, `DAGSTER_ADHOC_TRIGGER_CAUSE` in v2). dbt Cloud stores this on the run record.
  *Refs:* `dagster-dbt/cloud/resources.py:232`, `dagster-dbt/cloud_v2/client.py:290-291`

**Where the gaps are today:** `dagster-bigquery`, `dagster-redshift`, `dagster-postgres`, `dagster-mysql`, and — critically for our biggest use case — `dagster-dbt` (Core), where dbt opens the connection itself. Layer 1 closes those gaps by applying the same pattern uniformly.

## Layer 1: Default workload identification

Ships in every warehouse/compute integration. On by default. Opt-out only.

**Not a new pattern — we already do this for dbt Cloud.** `dagster-dbt-cloud` tags every job trigger with a `cause` field defaulting to `"Triggered via Dagster"` (`cloud/resources.py:232`, `cloud_v2/client.py:290-291`). It's been shipping for years and demonstrates the mechanism works. This proposal generalizes that same pattern to every other warehouse/compute integration.

### Payload and channels

Minimal default payload: `dagster:<version>`. No asset keys, no run IDs, no customer identifiers.

| Warehouse | Mechanism | Where it shows up |
|---|---|---|
| Snowflake | `QUERY_TAG` session parameter + `APPLICATION` client context | `ACCOUNT_USAGE.QUERY_HISTORY.QUERY_TAG` + `CLIENT_APPLICATION_NAME` |
| BigQuery | `job_labels` on every job (e.g. `origin=dagster`) | `INFORMATION_SCHEMA.JOBS.labels` |
| Databricks | Statement tags + JDBC application name | `system.query.history.client_application` |
| Redshift | `QUERY_GROUP` + JDBC application name | `STL_QUERY.querytxt` + system tables |
| Postgres / MotherDuck | `application_name` connection parameter | `pg_stat_activity`, MotherDuck telemetry |
| dbt Core (via adapter) | Ephemeral `--profiles-dir` with injected `query_tag` / `query_labels` / `statement_tags`, OR small `dagster_dbt_attribution` package overriding `set_query_tag` macro. **Same profile swap carries the Layer 2 Insights payload (opaque ID for reconciliation) when Insights is enabled — one mechanism, both layers, no competing overrides.** | Warehouse's own query history — same channels as above |
| dbt Cloud | `cause` field on job trigger API — **already shipping** (`cloud/resources.py:232` defaults to `"Triggered via Dagster"`) | dbt Cloud UI + analytics |

### Design principles

- **Opt-out, not opt-in.** Setting: `dagster.integrations.identify_workload = false`. Default on. Enterprises with sensitivity can turn off; most customers won't care.
- **Composability.** Customers already set their own `query_tag` in dbt profiles, `job_labels` in BQ configs. Must append/prefix, never overwrite.
- **PII discipline.** Default payload is `dagster:<version>` only — nothing customer-identifying. Anything richer (asset key, run ID, deployment ID) is Insights-tier, opt-in.
- **Zero code change for customers.** Ships in the integration itself. Customers upgrading get it automatically.

### Implementation targets

Every library that opens a warehouse/compute connection is a target. Each reads from a shared `dagster._core.workload_identity` helper (`get_dagster_workload_identifier() -> str | None`) so the opt-out flag and payload format live in one place; individual libraries only translate the identifier into their connector-specific mechanism.

Rows marked **Enhance** already ship baseline identification (see "What we already do"); the change adds a richer channel or standardizes the payload. Rows marked **New** have no attribution today. Snowflake / Databricks / dbt Cloud are prioritized above the "New" integrations because they represent our largest customer footprint and we want to deepen presence there.

| Library | Change | Mechanism |
|---|---|---|
| `dagster-snowflake` | **Enhance** (~15 lines) | Add QUERY_TAG to `session_parameters` in `_connection_args()` (`resources.py:331`) as a secondary channel carrying version + Layer 2 opaque_id when Insights is enabled. Keep `application = "DagsterLabs_Dagster"` unchanged (registered Snowflake Partner identifier — changing it could break partner-side telemetry). |
| `dagster-dbt` (Core) | **New** (~40 lines in `core/resource.py::cli()`) | Ephemeral profile via existing `profiles_dir` support (see pseudo-code below); same swap carries Layer 2 payload. Closes the dbt-orchestrating-Snowflake gap where dbt-snowflake opens the connection instead of us. Optional overlay package (`dagster_dbt_attribution`) for per-model granularity. |
| `dagster-dbt-cloud` | **Enhance** (~5 lines) | Standardize `cause` string to a stable prefix format like `dagster:<version>: <context>` in `cloud/resources.py:232` and the `DAGSTER_ADHOC_TRIGGER_CAUSE` constant in `cloud_v2/client.py`. Gives dbt Labs a reliable grep pattern for aggregation. |
| `dagster-databricks` | **Enhance** (TBD scope) | `product = "dagster-databricks"` + `product_version` already cover `WorkspaceClient` (`databricks.py:90`). Only additional work if we adopt `databricks-sql-connector` alongside (not used today per grep) — then `_user_agent_entry` there too. |
| `dagster-bigquery` | **New** (~20 lines across resource + IO manager) | Set `client_info.user_agent` on the client; inject `labels={"origin": "dagster"}` into every `QueryJobConfig`. |
| `dagster-redshift` | **New** (~15 lines) | `application_name` connection param + optional `SET QUERY_GROUP` on session open. |
| `dagster-postgres` / `dagster-mysql` | **New** (~10 lines each) | `application_name` connection param. |
| `dagster-dlt` | **New** (~20 lines) | Inject destination-specific tag via dlt config API. |
| `dagster-sling` | **New** (~20 lines) | Inject destination-specific tag via Sling config. |

**Non-targets:** `dagster-airbyte` and `dagster-fivetran` don't open the warehouse connection themselves — Airbyte/Fivetran do. Attribution there would be a partnership conversation with those vendors to tag Dagster-triggered syncs on their side, not a Dagster code change.

### Estimated scope

Bulk of the work is deciding the payload format + opt-out mechanism once and shipping the shared helper. Each library adds a small (~10-30 lines) change that reads from the helper and applies the connector-specific mechanism. Rolling ship one library at a time; Snowflake + BigQuery + dbt first.

### Example: dbt Core (the tricky one)

dbt Core is the only integration where we don't own the connector — we shell out to `dbt` and it opens the warehouse connection itself. The mechanism is to wrap every dbt CLI invocation with an ephemeral profile that composes Dagster attribution into the user's config, then point dbt at it via `--profiles-dir`.

```python
# Pseudo-code — real implementation lives in dagster-dbt.
# Reuses existing profiles_dir plumbing (core/resource.py:79, dbt_project.py:206).

def _apply_dagster_attribution(user_profile: dict, opaque_id: str | None) -> dict:
    """
    Compose a Dagster identifier into every output in the user's profile.
    - Layer 1 always: add `dagster:<version>` identifier.
    - Layer 2 (Insights enabled): also include opaque_id for reconciliation.
    Existing user-set tags/labels are preserved (append/merge, never overwrite).
    """
    identifier = f"dagster:{DAGSTER_VERSION}"
    if opaque_id:
        identifier += f":opaque_id={opaque_id}"

    for profile in user_profile.values():
        for target in profile.get("outputs", {}).values():
            match target.get("type"):
                case "snowflake":
                    existing = target.get("query_tag", "")
                    target["query_tag"] = f"{existing}|{identifier}" if existing else identifier
                case "bigquery":
                    labels = target.setdefault("labels", {})
                    labels["origin"] = "dagster"
                    if opaque_id:
                        labels["dagster_opaque_id"] = opaque_id
                case "databricks":
                    # Databricks specifics TBD — statement_tags via session_properties
                    # is the likely candidate; needs adapter-level verification.
                    ...
    return user_profile


def _run_dbt(command: str, args: list[str], user_profiles_dir: str, insights_context) -> int:
    user_profile = yaml.safe_load((Path(user_profiles_dir) / "profiles.yml").read_text())
    opaque_id = insights_context.new_opaque_id() if insights_context else None
    augmented = _apply_dagster_attribution(user_profile, opaque_id)

    with tempfile.TemporaryDirectory(prefix="dagster-dbt-") as tmp:
        (Path(tmp) / "profiles.yml").write_text(yaml.dump(augmented))
        return subprocess.call([*command, *args, "--profiles-dir", tmp])
```

**Design guarantees:**

- **User's original profile is never modified.** We write to a temp dir, point dbt there, clean up on exit. No credential persistence.
- **User-set values preserved.** `query_tag`, `labels`, `session_properties` are appended/merged, never overwritten. Someone with `query_tag: my_team` gets `my_team|dagster:1.9.5`.
- **Env-var interpolation still works.** We load with dbt's YAML loader (or preserve the raw `{{ env_var(...) }}` strings), so `password: "{{ env_var('SF_PWD') }}"` still resolves correctly at dbt's connection time.
- **One profile swap, both layers.** Layer 1 always adds the identifier. Layer 2 (when Insights is enabled) adds the opaque ID to the same tag — no second override.
- **Layer 2 reconciliation** for dbt-orchestrated Snowflake queries then works like today: backend polls `QUERY_HISTORY`, filters `WHERE QUERY_TAG LIKE 'dagster:%'`, extracts the `opaque_id=...` payload, joins to the asset that emitted the observation.

**Alternative path — overlay package.** For customers who prefer to control profile injection themselves (or need per-model attribution rather than per-session), ship a small `dagster_dbt_attribution` dbt package. It overrides `dbt-snowflake`'s `set_query_tag` macro via the documented `adapter.dispatch('set_query_tag', 'dbt')` mechanism, reads a Dagster-set env var, and applies the same identifier per-model rather than per-session. Users install once via `packages.yml`. Both paths coexist — ephemeral profile is the default, overlay package is opt-in for finer granularity.

## Layer 2: Insights refactor

### Where it's hardcoded today

**Backend consumer** — `dagster_cloud_backend/streamline/consumers/event_metrics.py:51`:

```python
OPAQUE_ID_METADATA_KEY_PREFIX = "dagster_snowflake_opaque_id:"
```

Only observations with metadata keys starting with this literal string are treated as query events. Nothing else can register a reconciled cost event.

**Metric definitions** — `dagster_cloud_backend/custom_metrics/metric_definitions.py`:

```python
BIGQUERY_RAW_BYTES_METRIC_NAME = f"{DAGSTER_USER_COST_METRIC_PREFIX}bigquery_bytes_billed"
SNOWFLAKE_CREDITS_METRIC_NAME  = f"{DAGSTER_USER_COST_METRIC_PREFIX}snowflake_credits"
DEFAULT_BIGQUERY_BYTES_COST_MULTIPLIER: float = 6.25 / 2**40  # $6.25 USD per TiB
```

Metric names and cost multipliers are enumerated per-warehouse. No registry. ~118 hardcoded `snowflake_*` / `bigquery_*` references across client, backend, and UI.

### Integrations this framework should support

None of these are committed work — this table is what would motivate the abstraction. If any of them land on a roadmap, each row maps to one of the four primitives below.

| Integration | Attribution shape |
|---|---|
| Databricks serverless SQL | Per-statement (primitive #1 or #2) |
| Databricks classic / job clusters | Cluster wall-time (primitive #3) |
| MotherDuck | Per-query (primitive #1 or #2) |
| Redshift | Per-query with tags (primitive #2) |
| K8s, ECS, ACA, Cloud Run | Container runtime (primitive #3) |

Under the current architecture, every row would be a fork of `snowflake_utils.py` or `bigquery_utils.py` plus a new hardcoded backend prefix plus a new UI column. Under the proposed framework, each is a registered strategy.

### Four cost attribution primitives

**1. Direct.** Provider tells you cost synchronously. Client attaches cost metadata to the asset materialization. No reconciliation.
Fits: BigQuery, Databricks serverless statements, MotherDuck.

**2. Reconciled.** Provider exposes cost after-the-fact keyed by an injected identifier. Client emits an observation with a correlation key; backend joins to the provider's billing/query history.
Fits: Snowflake (today), Redshift, Databricks serverless SQL.
*Channel reuse:* the correlation key rides on the Layer 1 identification channel — same `QUERY_TAG` / `job_labels` / `statement_tags`, richer payload. For **dbt Core** specifically, this means the ephemeral profile replacement used for Layer 1 identification also carries the Layer 2 payload — one profile swap covers both layers, not two competing overrides.

**3. Apportioned.** Provider bills by container/cluster wall-time. Backend knows which assets ran on which container during which window and divides cost.
Fits: K8s pods, ECS tasks, ACA containers, Cloud Run, Databricks classic clusters, DLT pipelines.

**4. Passive (warehouse-derived).** Backend polls the warehouse's own cost data (Snowflake `ACCOUNT_USAGE`, BigQuery `INFORMATION_SCHEMA.JOBS`, Databricks `system.query.history` + `system.billing.usage_stats`) and joins on table/schema/database names. No client-side hooks. Table-level attribution only (warehouse's own model — no pipeline granularity). Assets that later adopt Dagster orchestration upgrade to reconciled (pipeline-level) in the same UI.
Requires warehouse-level read on billing views; opt-in.

### API shape

Framework is **asset-centric**. Public API attaches to `AssetSpec` / `AssetKey`; every producer (`@asset`, `@dbt_assets`, dbt project components, future integrations) attaches strategies uniformly:

```python
@asset(cost_attribution=DatabricksSqlReconciliation(warehouse_id="..."))
def my_asset(): ...

# Same shape for AssetSpec, dbt component config, etc.
```

Backend consumer iterates `AssetSpec → strategy`. UI displays "asset X cost $Y" identically across producers.

Looking forward, this framework may also be able to support Connections as a consumer down the road — a natural extension worth exploring once the core is in place.

### Backend registry

Replace hardcoded `OPAQUE_ID_METADATA_KEY_PREFIX` with a registry:

```python
@dataclass
class CostAttributionStrategy:
    integration_id: str                     # "snowflake", "bigquery", "databricks", ...
    metadata_key_prefix: str
    attribution_mode: Literal["direct", "reconciled", "apportioned", "passive"]
    reconciler: Callable[..., Iterable[CostEvent]] | None
    cost_unit: CostUnit                     # credits, USD, bytes, pod-seconds

STRATEGY_REGISTRY: dict[str, CostAttributionStrategy] = { ... }
```

`event_metrics.py` iterates strategies instead of filtering on one hardcoded prefix. Each new integration registers a strategy; no core consumer change.

**Estimated size:** small. Mostly a refactor of the one consumer file and metric definitions. Unblocks everything else.

### UI

1. **Cost unit registry** (mirror of the backend). Each integration declares unit(s), display format, USD conversion. Adding Databricks DBUs, K8s pod-seconds becomes config, not a frontend PR.

2. **Attribution mode as user-visible metadata.** UX must communicate confidence:
   - Direct: **$0.12** exact
   - Reconciled: **$0.12** · reconciled from query history — exact but delayed
   - Apportioned: **~$0.12** · share of pod-cluster time — estimate
   - Passive: **~$0.12** · from warehouse billing — table-level only

   Users lose trust when numbers change silently.

3. **Multi-source cost breakdown per asset.** dbt-on-Snowflake under a K8s job = query cost + orchestration cost. Asset detail needs a breakdown ("Snowflake $0.08 + K8s $0.04"); graph view sums across providers.

**Chart shape:** point events (Snowflake queries) vs continuous time-series (K8s pods) don't fit today's per-materialization bar chart. Needs a normalized-cost-per-materialization view + a cost-timeline view.

## Sequencing

**Layer 1 — ship independently, no dependencies:**

1. **Design payload format + opt-out mechanism.** One-time design applied uniformly.
2. **Snowflake QUERY_TAG enhancement + `dagster-dbt` Core profile injection** — the biggest use case.
3. **`dagster-dbt-cloud` cause standardization** — small change, immediate partner-side benefit.
4. **BigQuery** attribution (new channel).
5. **Databricks** — evaluate whether we're adopting `databricks-sql-connector`; enhance only if so.
6. **Redshift, Postgres, MotherDuck, dlt, Sling** — rolling per integration.

**Layer 2 — parallel track, different owners:**

7. **Design doc — backend + UI registry together.**
8. **Backend registry PR.** Small, unblocking. Populate with current Snowflake + BigQuery strategies; no user-visible change.
9. **UI cost-unit registry PR.** Same shape, frontend. Existing charts read from registry; behavior unchanged.
10. **Databricks serverless SQL** — first integration on the new framework.
11. **Passive attribution prototype.** Cheapest ship since no client SDK.
12. **K8s apportionment prototype.** Once this works, ECS / ACA / Cloud Run / Databricks clusters are hours each.
13. **MotherDuck, Redshift** — pattern #1 / #2 integrations.

**Coordination point:** Layer 1's payload format must be extensible so Layer 2 uses the same channel with richer content (e.g., default payload `dagster:<version>` → Insights payload `dagster:<version>:<opaque_id>`). Otherwise we ship two parallel identification systems.

## Tradeoffs

**Layer 1 — default on vs opt-in:**

- *Default on* (recommended): partner attribution actually reaches critical mass. Opt-out available for the rare sensitive customer.
- *Opt-in*: safest but almost nobody opts in, so partner attribution never gets meaningful coverage. Defeats the point.

**Refactor before Databricks vs after:**

- *Before* (recommended): +1 sprint delay; every subsequent integration is small.
- *After*: Databricks ships faster on hardcoded pattern; MotherDuck + Redshift each become one-off hardcode PRs; `event_metrics.py:51` hardcode compounds.

**Full four-primitive abstraction upfront vs incremental:**

- *Full*: engineers add integrations independently; four-primitive shape makes warehouses + infra + passive coexist under one framework.
- *Incremental (#1 and #2 only)*: locks the shape before stress-testing apportionment and passive. Escape hatches → the pattern that got us here.

**Frontend registry all-at-once vs per-view:**

- *All-at-once*: adding an integration requires no UI PR.
- *Per-view*: some views registry, some hardcoded — worst of both. Avoid.

## Ask

1. **Directional approval on the two-layer model** — default identification (Layer 1) + Insights refactor (Layer 2).
2. **Layer 1 payload format.** Is `dagster:<version>` the right default? Do we want `dagster:<version>:<deployment_id>` (identifiable but not PII)?
3. **Layer 1 opt-out mechanism.** Settings-based? Env var? Per-integration config? Preference.
4. **Layer 2 approval** on the four-primitive model + registry approach.
5. **Backend refactor ownership** — I can draft the `event_metrics.py` + `metric_definitions.py` changes with engineering review, or engineering owns it with input from me. Preference?
6. **UI registry ownership.** Frontend team's call. Happy to write a follow-up doc scoped to the UI once (1) and (4) are agreed.
7. **Sequencing.** Layer 1 first, Layer 2 first, or parallel with different owners? Recommend parallel.

## Appendix: file pointers

- `dagster_cloud/dagster_insights/snowflake/snowflake_utils.py` — Snowflake client-side opaque ID injection (Layer 2)
- `dagster_cloud/dagster_insights/bigquery/bigquery_utils.py` — BigQuery direct metadata attachment (Layer 2)
- `dagster_cloud_backend/streamline/consumers/event_metrics.py:51` — hardcoded `OPAQUE_ID_METADATA_KEY_PREFIX` (Layer 2 target)
- `dagster_cloud_backend/custom_metrics/metric_definitions.py` — hardcoded per-provider metric names and cost multipliers (Layer 2 target)
- `python_modules/libraries/dagster-snowflake/` — Layer 1 target for Snowflake `QUERY_TAG` default
- `python_modules/libraries/dagster-bigquery/` — Layer 1 target for BigQuery `job_labels` default
- `python_modules/libraries/dagster-databricks/` — Layer 1 target for Databricks statement tags
