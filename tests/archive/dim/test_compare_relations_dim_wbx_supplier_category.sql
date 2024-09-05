{{config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>1' 
) }} 

{% set old_etl_relation=ref('conv_adr_supplier_category_dim') %} 

{% set dbt_relation=ref('dim_wbx_supplier_category') %} 

{{ ent_dbt_package.compare_relations( 

    a_relation=old_etl_relation, 
    b_relation=dbt_relation,
    exclude_columns=["UPDATE_DATE","UPDATED_BY"], 
    primary_key="UNIQUE_KEY",
    summarize=false
) }}

