
{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_sls_wtx_pricing') %} 

{% set dbt_relation= 'wbx_prod.R_EI_SYSADM.v_sls_wtx_pricing'  %} 

{% set filter_field= "COMPANY" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["load_date","update_date"],
    summarize=false 

)
}}