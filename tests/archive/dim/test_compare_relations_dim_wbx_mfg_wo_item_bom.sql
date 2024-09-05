{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation=ref('conv_mfg_wtx_wo_item_bom_dim') %}

{% set dbt_relation=ref('dim_wbx_mfg_wo_item_bom') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["LOAD_DATE","update_date","SOURCE_UPDATED_DATE","root_src_item_guid","parent_src_item_guid","comp_src_item_guid"],
    primary_key="UNIQUE_KEY",
    summarize=false
    )
}}

