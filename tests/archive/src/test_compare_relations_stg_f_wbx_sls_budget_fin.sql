{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation = source("FACTS_FOR_COMPARE","sls_wtx_fin_slsfcst_stg_bud21") %}

{% set dbt_relation=ref('stg_f_wbx_sls_budget_fin') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE"],
    summarize=false,
    primary_key="TRATYPCDE || COMCDE5D || CYR || CYRPER"
    )
}}