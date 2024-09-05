{{ config( 

  enabled=false, 

  severity = 'warn', 

  warn_if = '>0' 

) }} 


{% set a_relation=ref('conv_fin_wtx_prc_po_receipt_fact')%}

{% set b_relation=ref('fct_wbx_fin_prc_po_receipt') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["SUPPLIER_ADDRESS_NUMBER_GUID","ACCOUNT_GUID","BUSINESS_UNIT_ADDRESS_GUID","PAYMENT_TERMS_GUID","PAYEE_ADDRESS_NUMBER_GUID","ITEM_GUID","LOAD_STATUS","LOAD_DATE","UPDATE_DATE","BATCH_DATE","BATCH_NUMBER","BATCH_TYPE","ETL_BATCH_ID","PURCHASE_ORDER_GUID","CONTRACT_AGREEMENT_GUID","SUBLEDGER_GUID","SUBLEDGER_TYPE_DESC","BUYER_ADDRESS_NUMBER_GUID","PO_RECEIPT_GUID","PO_FACT_UNIQUE_KEY"],
    primary_key=UNIQUE_KEY,
    summarize=true
) }}
