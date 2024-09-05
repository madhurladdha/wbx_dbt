{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set a_relation=ref('conv_sls_wtx_gl_trade_fact') %}

{% set b_relation=ref('fct_wbx_sls_gl_trade') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["ACCOUNT_GUID","BUSINESS_UNIT_ADDRESS_GUID","ITEM_GUID"],
    summarize=false,
    primary_key="UNIQUE_KEY"
) }}
