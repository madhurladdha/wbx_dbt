{{ config(
  enabled=false,
  severity = 'warn'
) }}



{% set old_etl_relation = ref('fct_wbx_mfg_monthly_bom_snapshot') %} 

{% set dbt_relation= 'wbx_prod.fact.fct_wbx_mfg_monthly_bom_snapshot'  %} 

{% set filter_field= "root_company_code" %} 

{% set filter_values= "'WBX'"  %} 


{{ compare_relations_d365 (
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["LOAD_DATE","update_date","SOURCE_UPDATED_DATE","root_src_item_guid","parent_src_item_guid","comp_src_item_guid"],
    summarize=false
    )
}}

