"""Template vars for the commerce_core dbt component defs.yaml.

Referenced via `template_vars_module: .template_vars` in defs.yaml.
Provides an `automation_eager` var so `post_processing.assets[*].attributes`
can attach it via `"{{ automation_eager }}"`.
"""

import dagster as dg


@dg.template_var
def automation_eager() -> dg.AutomationCondition:
    """Eager: re-fire the asset as soon as any upstream materializes,
    provided every upstream is up to date. Applied to all commerce_core
    dbt assets so the ingestion → dim/fct cascade fires automatically
    on each 15-min ingestion schedule tick.
    """
    return dg.AutomationCondition.eager()
