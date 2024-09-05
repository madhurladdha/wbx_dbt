{{ config( 

  enabled=false, 

  severity = 'warn'

) }} 


{% set a_relation=ref('conv_dim_date_oc')%}

{% set b_relation=ref('dim_wbx_date_oc') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["LOAD_DATE","UPDATE_DATE"],
    primary_key="fiscal_date_id",
    summarize=false
) }}