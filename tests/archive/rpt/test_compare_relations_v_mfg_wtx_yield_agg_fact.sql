{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["weetabix","yield","aggregate","agg","wbx","fact","mfg","manufacturing"]

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_mfg_wtx_yield_agg_fact') %} 

{% set dbt_relation=ref('v_mfg_wtx_yield_agg_fact') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["LOAD_DATE","UPDATE_DATE"],
    summarize=false

) }} 