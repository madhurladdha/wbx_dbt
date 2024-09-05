{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation=ref('conv_sls_wbx_fc_snapshot_dim') %}

{% set dbt_relation=ref('dim_wbx_fc_snapshot') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE",'UPDATE_DATE'],
    summarize=false
    )
}}
