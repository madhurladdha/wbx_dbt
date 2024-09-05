{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_wtx_po_fact') %} 

{% set dbt_relation= 'wbx_prod.r_ei_sysadm.v_wtx_po_fact'  %} 

{% set filter_field= "po_order_company" %} 

{% set filter_values= "'WBX'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["source_date_updated","load_date","update_date","SOURCE_UPDATED_D_ID"],
    primary_key="po_order_number",
    summarize=false

)
}}