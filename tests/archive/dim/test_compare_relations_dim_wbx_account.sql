{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation=ref('conv_fin_account_dim') %}

{% set dbt_relation=ref('dim_wbx_account') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE","DATE_UPDATED","SOURCE_COST_CENTER"],
    primary_key="UNIQUE_KEY",
    summarize=false
    )
}}

