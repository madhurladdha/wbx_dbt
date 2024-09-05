{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_inv_wtx_ss_week_snapshot') %} 

{% set dbt_relation= 'wbx_prod.r_ei_sysadm.v_inv_wtx_ss_week_snapshot'  %} 

{% set filter_field= "source_company_code" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["source_date_updated","load_date","update_date"],
    primary_key="CASE_ITEM_NUMBER",
    summarize=false

)
}}