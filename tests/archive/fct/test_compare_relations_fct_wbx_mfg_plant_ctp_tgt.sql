{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 





{% set a_relation=source('R_EI_SYSADM','mfg_wtx_plant_ctp_tgt_fact')%}

{% set b_relation=ref('fct_wbx_mfg_plant_ctp_tgt') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["UPDATE_DATE","LOAD_DATE","UNIQUE_KEY","BUSINESS_UNIT_ADDRESS_GUID"],
    summarize=false
) }}