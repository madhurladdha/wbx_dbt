{{ config(enabled=false, severity='warn', warn_if='>0') }}

{% set old_etl_relation = ref('conv_fin_wtx_ar_custinv_hdr_fact') %}

{% set dbt_relation = ref('fct_wbx_fin_ar_custinv_hdr') %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=["AR_CUSTINV_HDR_GUID","CUSTOMER_ADDRESS_NUMBER_GUID","ACCOUNT_GUID","BUSINESS_UNIT_ADDRESS_GUID","PAYMENT_TERMS_GUID","GL_OFFSET_SRCCD","SOURCE_PAYOR_IDENTIFIER","PAYOR_ADDRESS_NUMBER_GUID","ORIGINAL_DOCUMENT_NUMBER","ORIGINAL_DOCUMENT_TYPE","ORIGINAL_DOCUMENT_COMPANY","ORIGINAL_DOCUMENT_PAY_ITEM","SUPPLIER_INVOICE_NUMBER","SALES_DOCUMENT_SUFFIX","REMARK_TXT","VOID_DATE","VOID_FLAG","DEDUCTION_REASON_CODE","BATCH_TYPE","BATCH_NUMBER","BATCH_DATE","LOAD_STATUS","LOAD_DATE","UPDATE_DATE"],
        primary_key="UNIQUE_KEY",
        summarize=true,
    )
}}
