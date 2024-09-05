{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation=ref('conv_wbx_mfg_item_variant') %}

{% set dbt_relation=ref('dim_wbx_mfg_item_variant') %}


{{ ent_dbt_package.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["SOURCE_UPDATED_DATE","LOAD_DATE","UPDATE_DATE",],
    primary_key="UNIQUE_KEY",
    summarize=false
    )
}}

