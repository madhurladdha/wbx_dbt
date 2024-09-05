{{ config( 
  enabled=false, 
  severity = 'warn',
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"), 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_inv_wtx_pct_week_snapshot') %} 

{% set dbt_relation= 'WBX_PROD.r_ei_sysadm.v_inv_wtx_pct_week_snapshot'  %} 

{% set filter_field= "SOURCE_COMPANY_CODE" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["load_date","update_date","phi_currency","base_currency","ITEM_ALLOCATION_KEY","DESCRIPTION"],

    primary_key=["SOURCE_ITEM_IDENTIFIER","week_start_dt","week_end_dt"],
    summarize=false 

)
}}