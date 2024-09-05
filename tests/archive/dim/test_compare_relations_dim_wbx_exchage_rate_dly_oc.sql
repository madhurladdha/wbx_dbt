{{ config( 

  enabled=false, 

  severity = 'warn'

) }} 

{% set a_relation=ref('conv_currency_exch_rate_dly_dim')%}

{% set b_relation=ref('dim_wbx_exchange_rate_dly_oc') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=[""],
    primary_key="CURR_CONV_RATE_I",
    summarize=false
) }}

