{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


{% set a_relation=ref('conv_fct_wbx_prc_agreement_balance_snapshot')%}

{% set b_relation=ref('fct_wbx_prc_agreement_balance_snapshot') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["SOURCE_UPDATED_DATE", "SOURCE_UPDATED_TIME"],
    
    summarize=false
) }}

