{{ config( 
    enabled=false,
    severity = 'warn', 
    warn_if = '>1'
) }} 

 

{% set old_etl_relation=ref('conv_wbx_prc_ppv_item_inflation') %} 

 

{% set dbt_relation=ref('fct_wbx_prc_ppv_item_inflation') %} 

 

{{ent_dbt_package.compare_relations( a_relation=old_etl_relation, b_relation=dbt_relation,
exclude_columns=[
    "LOAD_DATE",
    "version_dt"
    ],
    summarize=true
    ) }} 