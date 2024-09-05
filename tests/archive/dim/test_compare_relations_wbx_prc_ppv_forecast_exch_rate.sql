{{ config( 
    enabled=false,
    severity = 'warn', 
    warn_if = '>1'
) }} 

 

{% set old_etl_relation=ref('conv_wbx_prc_ppv_forecast_exch_rate') %} 

 

{% set dbt_relation=ref('dim_wbx_prc_ppv_forecast_exch_rate') %} 

 

{{ent_dbt_package.compare_relations( a_relation=old_etl_relation, b_relation=dbt_relation,
exclude_columns=[
    "LOAD_DATE",
    "version_dt"
    ],
    summarize=true
    ) }} 