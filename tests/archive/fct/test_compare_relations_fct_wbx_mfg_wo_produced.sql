{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


{% set a_relation=ref('conv_fct_wbx_mfg_wo_produced')%}

{% set b_relation=ref('fct_wbx_mfg_wo_produced') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=[ "SOURCE_LOAD_DATE","SOURCE_UPDATED_DATE","LOAD_DATE","UPDATE_DATE","SOURCE_UPDATED_TIME","BUSINESS_UNIT_ADDRESS_GUID","ITEM_GUID", "CUSTOMER_ADDRESS_NUMBER_GUID",],
    primary_key='unique_key',
    summarize=false
) }}