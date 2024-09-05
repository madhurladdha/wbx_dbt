{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation = source("FACTS_FOR_COMPARE","prc_wtx_item_inflation_stg") %}

{% set dbt_relation=ref('stg_d_wbx_inflation') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE"],
    summarize=false
    )
}}