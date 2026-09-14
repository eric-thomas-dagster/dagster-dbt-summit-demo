# dbt Summit 2026 Demo — Stage Walkthrough

**One graph, four code locations, three merges.** Every feature from the
dagster-dbt PR stack (#26391 / #26392 / #26393) has a natural home in this
demo, and the audience sees seven data-stack tools (Fivetran, dlt, dbt × 2,
Census, Hightouch, PowerBI) stitched into one lineage — no live SaaS APIs
required, but every asset materializes on click.

## Setup (before you go on stage)

```bash
cd /Users/ericthomas/dbt-features-demo/demo
uv sync
cd dbt_projects/subscriptions_analytics && dbt deps && cd ../..
./run-demo.sh
```

**Important:** boot via `./run-demo.sh` (or manually set `DAGSTER_HOME=$(pwd)/.dagster_home`).
Freshness policies stay disabled in preview mode unless `dagster.yaml` sets
`freshness: enabled: True` — the checked-in `.dagster_home/dagster.yaml` has that
line, but `dagster dev` uses an ephemeral home directory by default and ignores it.
Without DAGSTER_HOME set, Arc 2 (source freshness) has nothing to show.

Open http://localhost:3000. The location dropdown shows `ingestion`,
`commerce_core`, `subscriptions_analytics`, `activation` — four locations
mirroring the four data-platform layers. Owner tags render two teams
(Platform Team + Growth Team) across those locations.

## Arc 1 — "Your data platform, one graph" (2 minutes)

Open the asset lineage. The full graph:

```
Fivetran raw_stripe.{customers,charges}    ─┐
dlt      raw_events.{signups,page_views}   ─┴─► commerce_core dbt project
                                                (staging → dim/fct)
                                                     │
                                                     ▼ (dbt mesh)
                                            subscriptions_analytics
                                            (mrr, churn, ltv, semantic layer)
                                                     │
              ┌──────────────────────────────────────┼──────────────────────┐
              ▼                                      ▼                      ▼
   Census → Salesforce (MRR)          Hightouch → Braze (churn/LTV)   PowerBI dashboards
```

**Punchline:** every non-dbt tool is a **subclass** of the official
component (`FivetranAccountComponent`, `DltLoadCollectionComponent`,
`CensusComponent`, `HightouchSyncComponent`, `PowerBIWorkspaceComponent`).
Point to `src/demo/components/commerce_fivetran_ingestion.py` on the screen:

> "This is the whole difference between demo mode and prod — `if not
> self.demo_mode: return super().build_defs(context)`. Flip the flag,
> supply real credentials, and you're running against the real Fivetran API.
> Zero code change on any downstream asset."

## Arc 2 — Feature 1b (`build_after` model freshness) (2 min)

Two dbt-model freshness policies show up in the UI, both derived from dbt-native declarations via `enable_freshness_policies: true`:

- `commerce_core.dim_customers` — `build_after: 24h` in schema.yml → `TimeWindowFreshnessPolicy(fail_window=24h)` in Dagster
- `subscriptions_analytics.mrr` — `build_after: 6h` in schema.yml → same story, tighter window

> "Declare freshness once in dbt. Dagster derives the policy. Both `dim_customers` and `mrr` now have freshness SLOs that light up green/yellow/red in the UI — no code, no separate Dagster config."

**Note (backstage — do not say on stage):** Source freshness (`sources.yml` warn_after / error_after — Feature 1a) is set on commerce_core's dbt spec but doesn't render on the merged Fivetran/dlt nodes because Dagster core's cross-code-location merge prefers materialization-side specs (which have `freshness_policy=None`). Post-Summit follow-up: fix `RemoteWorkspaceAssetNode.freshness_policy` in Dagster core to prefer non-None from either side. Feature 1b (models) works today; Feature 1a source visibility is the followup.

## Arc 2b (skip on stage) — Feature 1a (source ↔ Fivetran merge across locations)

Click on `raw_stripe.customers`. Show the asset detail panel:

- Materializable via **Fivetran** (kinds: `fivetran`, `snowflake`)
- **Freshness policy** derived from dbt's `sources.yml` (warn 12h / error 24h)
- **Column schema** from dbt's contract
- **Source-freshness observations** wired via `enable_source_freshness_observations`

> "Two systems, one node. Fivetran contributes the *how it lands*. dbt
> contributes the *policy for how fresh it should be*. Dagster stitches them
> at the remote-graph level because they live in different code locations —
> `ingestion` owns Fivetran; `commerce_core` owns the dbt source. This is
> Feature 1a from PR #26391."

Click **Materialize**. 200 rows land in DuckDB. Show the resulting
`AssetMaterialization` event — `rows_synced`, `fivetran_status: succeeded`,
`demo_mode: true`. Real event log; real UI; faked API only.

## Arc 3 — Feature 5 (dbt mesh across locations) (2 min)

Switch to `subscriptions_analytics`. Click on `mrr`. Show the lineage:

- Upstream `fct_orders` shows as a **stub asset** owned by commerce_core's location
- Downstream lineage flows through Census / Hightouch / PowerBI

> "`fct_orders` lives in commerce_core. Growth Team's project depends on it
> via `external_packages: [commerce_core]` — one YAML line. Dagster emits a
> stub spec that auto-merges with commerce_core's real asset at the
> remote-graph level. Materialization ownership stays with the Platform
> Team; lineage stitches across teams for free. Feature 5."

## Arc 4 — Feature 4 (semantic layer + saved queries) (1 min)

In `subscriptions_analytics`, filter by kind: `semantic_model`, `metric`,
`saved_query`. Point to `sem_mrr`, `metric_mrr`, `metric_paying_customers`,
`mrr_last_12_months`, `mrr_summary`.

> "The whole semantic layer is first-class in the graph — semantic models,
> metrics, and saved queries all render as observable assets. Downstream BI
> can click through to see exactly which dbt model defines the metric they're
> consuming. Feature 4."

## Arc 5 — Feature 3 (exposures ↔ activation merge across locations) (1 min)

Click on `salesforce/account_mrr` (Census sync).

- Materializable via the **Census** component
- Description, owner, URL, maturity, `dbt/tags` — **all from dbt's `exposures.yml`**

> "Growth Team declared this exposure in dbt as the *canonical statement*
> that MRR feeds Salesforce. Activation lives in a separate Dagster code
> location. Because `meta.dagster.asset_key` on the exposure matches the
> Census asset key, the two nodes merge — dbt's declaration + activation's
> materialization, one asset in the UI. Feature 3."

## Arc 6 — Feature 3.6 (state-aware CI tagging) — the live demo moment

Show `dim_customers`. Its tag row includes `dbt/state=unchanged`.

Now, **live on stage**:

```bash
# In a terminal off to the side
vim dbt_projects/commerce_core/models/marts/dim_customers.sql
# Add a comment. Save.
cd dbt_projects/commerce_core && dbt parse && cd ../..
```

Reload the asset. Tag flips to `dbt/state=modified`.

> "One SQL edit. `dbt parse` compares checksums against
> `prod_state/manifest.json` (the last-shipped snapshot). Every affected
> model gets tagged. That tag is **programmatically selectable** —
> `AssetSelection.tag('dbt/state', 'modified')` gives you a slim-CI job
> selecting only what changed. This is what PR #26391's `state_manifest_path`
> restored to the stack after customer feedback — [customer signal in
> Slack](https://dagsterlabs.slack.com/archives/C08SBET4RM4/p1787655847764569)."

Bonus talking point:

> "For `@dbt_assets` users who don't adopt the component, the same helper
> `apply_dbt_state_tags(specs, current_manifest, state_manifest_path)` is
> exported as a public helper on PR #26393. Same tag, same selection, one
> function call away."

## Arc 7 — Feature 8 (value-carrying column tags) (30 sec)

Click a column on `dim_customers`. Point to `email` column tags:

- `pii: high`
- `data_class: personal`

> "Value-carrying tags per column — not just flags. Governance teams can
> filter the whole warehouse for `pii: high` columns and see exactly which
> assets are impacted. Feature 8."

## Arc 8 — Automation (AutomationCondition.eager) — closing (30 sec)

In the graph view, filter by tag `automation_condition:eager`. The 5
activation assets light up.

> "Every downstream activation auto-refreshes when its upstream mart
> advances. Finance's Salesforce MRR field, Growth's Braze audiences, the
> exec PowerBI dashboard — all `AutomationCondition.eager()` in YAML.
> Materialize `mrr` and the graph cascades through activation without
> anyone touching a schedule."

## Backup: things to point at if asked

- **Where is demo mode?** `src/demo/components/*.py` — 5 subclasses, each
  <150 lines, `if not self.demo_mode: return super().build_defs(context)`
  as the only demo-vs-prod fork
- **How does the graph flow end-to-end?** `mock_data.py` uses `Faker` to
  produce real DataFrames; `WarehouseResource` writes them to DuckDB; dbt
  reads DuckDB for real; activation fakes only the SaaS API call.
- **Why 4 code locations?** All three cross-location merges (Feature 1a
  source↔Fivetran, Feature 5 mesh stubs, Feature 3 exposure↔activation)
  only fire at the **remote asset graph** level, not at single-Definitions
  merge. See `workspace.yaml`.
- **PR references:** #26391 (Core), #26392 (Cloud), #26393 (Public helpers).
