{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_fin_prc_po') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_fin_prc_po'  %} 

{% set filter_field= "PO_ORDER_COMPANY" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["LOAD_DATE","UPDATE_DATE","SOURCE_DATE_UPDATED","SOURCE_UPDATED_D_ID"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 