{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 



{% set a_relation=source('R_EI_SYSADM','mfg_wtx_plant_wc_weekday_stg')%}

{% set b_relation=ref('stg_f_wbx_mfg_plant_wc_weekday') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["UPDATE_DATE","LOAD_DATE"],
    summarize=false
) }}