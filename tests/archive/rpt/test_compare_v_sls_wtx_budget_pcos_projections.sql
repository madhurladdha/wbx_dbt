{{ config( 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  enabled=false,
  tags=["sales","budget"]

) }} 


{% set old_etl_relation= source("FACTS_FOR_COMPARE_DEV","v_sls_wtx_budget_pcos_projections") %} 

{% set dbt_relation=ref('v_sls_wtx_budget_pcos_projections') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    summarize=true

) }} 