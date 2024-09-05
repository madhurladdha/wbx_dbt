{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_sls_order') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_sls_order'  %} 

{% set filter_field= "division" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["LOAD_DATE","UPDATE_DATE","CWT_QUANTITY_CONFIRMED" ,"SOURCE_OBJECT_ID","SOURCE_LEDGER_TYPE"], 

    primary_key="UNIQUE_KEY",
    summarize=true

) }} 
