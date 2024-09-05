{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 





{% set a_relation=ref('conv_fin_wtx_ap_voucher_hdr_fact')%}

{% set b_relation=ref('fct_wbx_fin_ap_voucher_hdr') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["AP_VOUCHER_HDR_GUID","SUPPLIER_ADDRESS_NUMBER_GUID","ACCOUNT_GUID","BUSINESS_UNIT_ADDRESS_GUID","PAYMENT_TERMS_GUID","PAYEE_ADDRESS_NUMBER_GUID","LOAD_STATUS","LOAD_DATE","UPDATE_DATE","BATCH_DATE","BATCH_NUMBER","BATCH_TYPE","ETL_BATCH_ID"],
    primary_key=None,
    summarize=true
) }}