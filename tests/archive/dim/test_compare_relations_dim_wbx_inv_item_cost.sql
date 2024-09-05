{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 

{% set old_etl_relation=ref('conv_inv_wtx_item_cost_dim') %} 

{% set dbt_relation=ref('dim_wbx_inv_item_cost') %} 


{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["LOAD_DATE","UPDATE_DATE","source_updated_d_id","expir_date","eff_date","eff_d_id","expir_d_id","source_updated_date"], 
    primary_key="UNIQUE_KEY",
    summarize=false 
) }} 