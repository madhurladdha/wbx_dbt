{{ config( 

  enabled=false, 

  severity = 'warn'

) }} 


{% set a_relation=ref('conv_uom_factor')%}

{% set b_relation=ref('dim_wbx_uom') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=[],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}