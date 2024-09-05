{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation=ref('conv_mfg_wtx_mnthly_bom_snapshot') %}

{% set dbt_relation=ref('fct_wbx_mfg_monthly_bom_snapshot') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE","update_date","SOURCE_UPDATED_DATE","root_src_item_guid","parent_src_item_guid","comp_src_item_guid"],
    summarize=false
    )
}}

