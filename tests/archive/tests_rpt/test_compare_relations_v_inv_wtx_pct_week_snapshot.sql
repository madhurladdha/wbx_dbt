{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["inventory", "wbx", "manufacturing", "percent", "weekly", "agg"]

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_inv_wtx_pct_week_snapshot') %} 

{% set dbt_relation=ref('v_inv_wtx_pct_week_snapshot') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["item_guid"],
    summarize=false

) }} 
