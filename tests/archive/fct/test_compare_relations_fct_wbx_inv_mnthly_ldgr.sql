{{ config( 
    enabled=false,
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    severity = 'warn', 
    warn_if = '>1' 
) }} 

{% set a_relation=ref('conv_inv_wtx_mnthly_ldgr_fact')%}

{% set b_relation=ref('fct_wbx_inv_mnthly_ldgr') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["update_date","load_date","business_unit_address_guid","lot_guid","location_guid","item_guid","address_guid"],
    primary_key=UNIQUE_KEY,
    summarize=true
) }}