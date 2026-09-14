# Customer reply draft — Slack thread

**Thread:** https://dagsterlabs.slack.com/archives/C08SBET4RM4/p1787655847764569

---

## Draft (paste into Slack)

Hey — great question, and honestly a really useful one. You've identified a real gap:

**On `changed_in_branch`:** you're right that `AssetSelection.from_string('changed_in_branch:"ANY"')` doesn't resolve programmatically today — `ChangedInBranchAssetSelection.resolve_inner` is a `NotImplementedError` because that diff currently only runs inside Dagster+ branch-deployment machinery. Making it resolvable in user code (via git diff against a base ref, or against a stored manifest, or via the Dagster+ API) is on our radar; I'm going to file a proper roadmap item for it and follow up here. No promises on timing yet, but it's a legitimate ask that affects more than just dbt.

**Short-term, for dbt specifically:** we have a feature landing in the next `dagster-dbt` release (part of PR stack #26391 → #26393) that solves your exact three complaints on the dbt side:

- **`state_manifest_path`** on `DbtProjectComponent` and `DbtCloudComponent`. You point it at a snapshotted prod `manifest.json`, and every dbt model spec gets tagged `dbt/state=modified|unchanged|new` — comparing per-model `checksum.checksum`. The tag lands on the spec *during creation*, so your custom `DbtProjectComponentTranslator`'s asset-key remapping applies natively (fixes your complaint #2).

- **Selection is fully programmatic:** `AssetSelection.tag("dbt/state", "modified")`. Works in `define_asset_job`, automation conditions, sensors, alerts — anywhere `AssetSelection` is accepted (fixes complaint #3, no duplicated diff work).

- **Bonus for `@dbt_assets` users:** if you don't want to adopt `DbtProjectComponent`, PR #26393 exposes `apply_dbt_state_tags(specs, current_manifest, state_manifest_path)` as a public helper. One line to add to your existing `@dbt_assets` setup, same behavior, same selection.

Full transparency: we actually pulled `state_manifest_path` from the PR earlier this month because we thought Dagster+ branch deployments made it redundant — turns out that was wrong for exactly the case you hit (programmatic selection). Your thread is why it's back. Thanks for filing this; it made the argument for us better than we could internally.

Happy to walk through the setup once the PRs merge, or if you want a preview from the branch let me know. And on the "only covers dbt assets" side — the roadmap item for general `changed_in_branch` will be the answer, but I'll flag we're not there yet.

---

## Notes for context (don't paste)

- Slack thread saved to memory as `reference_state_manifest_customer_signal.md`
- Customer signal now referenced in PR #26391 body under "Update — 2026-08-27"
- Reply intentionally does NOT promise a Core-side fix for `changed_in_branch` — user preference from this session ("we can say we are working on it"). If pressed, "filing a roadmap item" is the honest position.
- Tone: candid about the earlier removal being wrong, thankful for the signal, offering to preview from the branch. Feels like it fits a Dagster support voice.
- If you want to shorten for Slack, the core paragraphs are #2 (`state_manifest_path`) + #3 (selection) + #4 (`apply_dbt_state_tags`). Everything else is context.
