{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set a_relation=ref('conv_itm_lot_master_dim') %}

{% set b_relation=ref('dim_wbx_lot') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["LOAD_DATE","UPDATE_DATE","LOT_EXPIRED_FLAG"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}
