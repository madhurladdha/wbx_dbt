{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["pricing"]

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_wtx_pricing') %} 

{% set dbt_relation=ref('v_wtx_pricing') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=[ ],
    summarize=true

) }} 