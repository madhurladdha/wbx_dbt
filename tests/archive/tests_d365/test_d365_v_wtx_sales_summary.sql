{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_wtx_sales_summary') %} 

{% set dbt_relation= 'wbx_prod.r_ei_sysadm.v_wtx_sales_summary'  %} 

{% set filter_field= "SALES_ORDER_COMPANY" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["LOAD_DATE","UPDATE_DATE","SOURCE_UPDATED_TIME","SOURCE_UPDATED_D_ID"], 
    summarize=true 

) }} 