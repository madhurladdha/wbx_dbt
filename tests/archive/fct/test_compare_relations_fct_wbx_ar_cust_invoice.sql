{{ config(enabled=false, severity='warn', warn_if='>0') }}

{% set old_etl_relation = ref('conv_fin_wtx_ar_cust_invoice_fact') %}

{% set dbt_relation = ref('fct_wbx_fin_ar_cust_invoice') %}
/* Currently excluding PHI_TAX_AMT as it has small rounding differences */
{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=["AR_CUSTINV_HDR_GUID","AR_CUSTOMER_INVOICE_GUID","ITEM_GUID","CUSTOMER_ADDRESS_NUMBER_GUID","ACCOUNT_GUID","BUSINESS_UNIT_ADDRESS_GUID","PAYMENT_TERMS_GUID","PAYOR_ADDRESS_NUMBER_GUID","LOAD_STATUS","LOAD_DATE","UPDATE_DATE","AR_CUSTINV_HDR_UNIQUE_KEY","PHI_TAX_AMT"],
        primary_key="UNIQUE_KEY",
        summarize=true,
    )
}}
