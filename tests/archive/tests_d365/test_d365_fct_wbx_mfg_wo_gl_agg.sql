{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('fct_wbx_mfg_wo_gl_agg') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_mfg_wo_gl_agg'  %} 

{% set filter_field= "DOCUMENT_COMPANY" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["LOAD_DATE","UPDATE_DATE"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 