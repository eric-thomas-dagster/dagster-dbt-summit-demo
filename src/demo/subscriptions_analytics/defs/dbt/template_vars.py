"""Template vars for the subscriptions_analytics dbt component defs.yaml."""

import dagster as dg


@dg.template_var
def automation_eager() -> dg.AutomationCondition:
    """Eager on the downstream project — refires when commerce_core marts
    advance (via mesh) or when a subs_analytics model's SQL changes.
    """
    return dg.AutomationCondition.eager()
