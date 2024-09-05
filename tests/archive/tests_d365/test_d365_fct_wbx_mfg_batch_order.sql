{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_mfg_batch_order') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_mfg_batch_order'  %} 

{% set filter_field= "SOURCE_COMPANY" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["SOURCE_UPDATED_DATE","SOURCE_UPDATED_TIME","ITEM_GUID","BUSINESS_UNIT_ADDRESS_GUID"],

    primary_key="UNIQUE_KEY",
    summarize=false

)
}}