{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_prc_ppv') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_prc_ppv'  %} 

{% set filter_field= "COMPANY_CODE" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["LOAD_DATE"],

    primary_key="SOURCE_ITEM_IDENTIFIER",
    summarize=true

)
}}