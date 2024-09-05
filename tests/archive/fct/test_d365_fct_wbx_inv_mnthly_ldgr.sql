{{ config( 
    enabled=false,
    snowflake_warehouse= env_var("DBT_WBX_SF_WH"),
    severity = 'warn', 
    warn_if = '>1' 
) }} 


{% set old_etl_relation = ref('fct_wbx_inv_mnthly_ldgr') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_inv_mnthly_ldgr'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 


{{ compare_relations_d365( 

    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["update_date","load_date","business_unit_address_guid","lot_guid","location_guid","item_guid","address_guid"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}