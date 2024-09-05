{{ config( 

  enabled=false, 

  severity = 'warn'

) }} 


{% set a_relation=ref('conv_sls_wtx_promo_dim')%}

{% set b_relation=ref('dim_wbx_promo') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["UPDATE_DATE"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}


