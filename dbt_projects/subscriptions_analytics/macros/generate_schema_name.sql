{#
  Cross-project schema resolution for the mesh boundary.

  commerce_core is installed as a local dbt package in this project (see
  packages.yml). Its models were materialized by commerce_core's own dbt run
  into schema `commerce_core`. When subscriptions_analytics models `ref()`
  those imported models, dbt would default them to this project's target
  schema (`subscriptions_analytics_...`) — WRONG, because the actual tables
  live at `commerce_core.*`. This macro routes any ref lookup for a model
  in the commerce_core package to the `commerce_core` schema instead.

  In a dbt Cloud enterprise setup, `dependencies.yml` + two-arg `ref()`
  handles this natively via cross-project project references. This macro
  is the dbt Core equivalent for local demo runs.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if node.package_name == 'commerce_core' -%}
        commerce_core
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
