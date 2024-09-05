{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["finance","gl_pl","sales"]

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_fin_wtx_gl_pl_fact') %} 

{% set dbt_relation=ref('v_fin_wtx_gl_pl_fact') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["TARGET_ACCOUNT_IDENTIFIER","SOURCE_DATE_UPDATED","PHI_CONV_RT","OC_PHI_LEDGER_AMT",
    "TRADE_TYPE_DESC"],
    summarize=true

) }} 
