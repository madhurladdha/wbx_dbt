{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_prc_item_category') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_prc_item_category'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["update_date","load_date","MASTER_PLANNING_FAMILY_CODE","INGREDIENT_SIZE_CODE", "PACKAGING_DIE_SIZE_CODE", "LABEL_OWNER", "ORGANIC", "KOSHER", "GMO", "ALLERGEN", "CERTIFICATION", "SENSITIZERS", "RISK_FLAG", "CO_PACK_MAN","SAFETY_STOCK"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
