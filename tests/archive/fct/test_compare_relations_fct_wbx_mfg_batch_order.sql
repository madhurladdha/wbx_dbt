{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


{% set a_relation=ref('conv_fct_wbx_mfg_batch_order')%}

{% set b_relation=ref('fct_wbx_mfg_batch_order') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["SOURCE_UPDATED_DATE","SOURCE_UPDATED_TIME","ITEM_GUID","BUSINESS_UNIT_ADDRESS_GUID",
    ],
    primary_key=None,
    summarize=false
) }}

