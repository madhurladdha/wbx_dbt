{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["finance","gl","gl_trans","po"]

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_wtx_fixed_asset') %} 

{% set dbt_relation=ref('v_wtx_fixed_asset') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["project_guid"
                    ],
    summarize=true

) }} 