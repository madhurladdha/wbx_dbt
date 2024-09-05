{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_plant_dc') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_plant_dc'%} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["DATE_UPDATED","DATE_INSERTED","PERSON_RESPONSIBLE","PROGRAM_ID","ENTITY_LEVEL_1","ENTITY_LEVEL_2","DEFAULT_CURRENCY_CODE","ALT_SOURCE_BUSINESS_UNIT_CODE"], 
    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
