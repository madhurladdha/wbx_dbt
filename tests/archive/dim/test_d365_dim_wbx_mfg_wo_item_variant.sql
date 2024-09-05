{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_mfg_item_variant') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_mfg_item_variant'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["SOURCE_UPDATED_DATE","LOAD_DATE","UPDATE_DATE"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
