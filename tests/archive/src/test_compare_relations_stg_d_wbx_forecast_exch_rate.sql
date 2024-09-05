{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation = source("FACTS_FOR_COMPARE","prc_wtx_forecast_exch_rate_stg") %}

{% set dbt_relation=ref('stg_d_wbx_prc_forecast_exch_rate') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE"],
    primary_key="scenario",
    summarize=false
    )
}}
