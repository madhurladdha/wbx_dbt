{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_wtx_product_hierarchy') %} 

{% set dbt_relation= 'wbx_prod.R_EI_SYSADM.v_wtx_product_hierarchy'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["LOAD_DATE","DATE_UPDATED"], 
    summarize=false

) }} 
