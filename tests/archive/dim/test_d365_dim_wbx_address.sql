{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_address') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_address'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["DATE_INSERTED","DATE_UPDATED","CONTACT_2_EMAIL","PROGRAM_ID","ACTIVE_INDICATOR","ADDRESS_GUID","COMPANY_CODE","COMPANY_CODE_GUID"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
