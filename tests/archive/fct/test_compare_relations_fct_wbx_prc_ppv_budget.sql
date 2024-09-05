{{ config( 
    enabled=false,
    severity = 'warn', 
    warn_if = '>1'
) }} 

 

{% set old_etl_relation=ref('conv_prc_ppv_wbx_budget') %} 

 

{% set dbt_relation=ref('fct_wbx_prc_ppv_budget') %} 

 

{{ent_dbt_package.compare_relations( a_relation=old_etl_relation, b_relation=dbt_relation,
exclude_columns=[
    "LOAD_DATE",
    "version_dt",
    "item_guid"
    ],
    summarize=true
    ) }} 