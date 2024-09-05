{{ config( 

  enabled=false, 

  severity = 'warn'

) }} 


{% set a_relation=ref('conv_ref_payment_terms_xref')%}

{% set b_relation=ref('xref_wbx_payment_terms') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["LOAD_DATE","UPDATE_DATE"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}

 