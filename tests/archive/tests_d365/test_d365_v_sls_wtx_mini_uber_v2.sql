
{{ config( 
  enabled=false, 
  severity = 'warn', 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_sls_wtx_mini_uber_v2') %} 

{% set dbt_relation= 'wbx_prod.R_EI_SYSADM.v_sls_wtx_mini_uber_v2'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["UK_HOLIDAY_FLAG","DAY_OF_MONTH_BUSINESS","DAY_OF_MONTH_ACTUAL"],
    primary_key='SOURCE_ITEM_IDENTIFIER',
    summarize=false 

)
}}