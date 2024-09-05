{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_inv_aging') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_inv_aging'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=[
            "BUSINESS_UNIT_ADDRESS_GUID","LOCATION_GUID","LOT_GUID","LOAD_DATE","UPDATE_DATE","ITEM_GUID","source_updated_d_id"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

)
}}