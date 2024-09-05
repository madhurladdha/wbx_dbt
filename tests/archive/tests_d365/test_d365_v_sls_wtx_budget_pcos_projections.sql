
{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_sls_wtx_budget_pcos_projections') %} 

{% set dbt_relation= 'wbx_prod.R_EI_SYSADM.v_sls_wtx_budget_pcos_projections'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["BASE_EXT_ING_COST","BASE_EXT_PKG_COST","BASE_EXT_BOUGHT_IN_COST","BASE_EXT_ING_AMT","BASE_EXT_PKG_AMT"],
    primary_key='SOURCE_ITEM_IDENTIFIER',
    summarize=true 

)
}}