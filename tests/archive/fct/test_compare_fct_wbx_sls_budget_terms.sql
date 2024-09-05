{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


{% set a_relation=ref('conv_sls_wtx_budget_terms_fact')%}

{% set b_relation=ref('fct_wbx_sls_budget_terms') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=[""],
    primary_key=None,
    summarize=true
) }}