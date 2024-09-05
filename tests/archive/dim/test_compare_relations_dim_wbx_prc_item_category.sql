{{config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 

{% set old_etl_relation=ref('conv_itm_procurement_category_dim') %} 

{% set dbt_relation=ref('dim_wbx_prc_item_category') %} 

{{ ent_dbt_package.compare_relations( 

    a_relation=old_etl_relation, 
    b_relation=dbt_relation,
    exclude_columns=["update_date","load_date","MASTER_PLANNING_FAMILY_CODE","INGREDIENT_SIZE_CODE", "PACKAGING_DIE_SIZE_CODE", "LABEL_OWNER", "ORGANIC", "KOSHER", "GMO", "ALLERGEN", "CERTIFICATION", "SENSITIZERS", "RISK_FLAG", "CO_PACK_MAN","SAFETY_STOCK"], 
    primary_key="UNIQUE_KEY",
    summarize=false
) }}