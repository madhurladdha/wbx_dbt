{{ config(enabled=false, severity='warn', warn_if='>0') }}

{% set old_etl_relation = ref('conv_fin_wtx_ap_pymt_dtl_fact') %}

{% set dbt_relation = ref('fct_wbx_fin_ap_payment_dtl') %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=["AP_PYMT_DETAIL_GUID","AP_VOUCHER_HDR_GUID","ACCOUNT_GUID","BUSINESS_UNIT_ADDRESS_GUID","PAYEE_ADDRESS_NUMBER_GUID","BATCH_TYPE","BATCH_NUMBER","BATCH_DATE","LOAD_STATUS","LOAD_DATE","UPDATE_DATE","AP_VOUCHER_HDR_UNIQUE_KEY","ETL_BATCH_ID"],
        primary_key="UNIQUE_KEY",
        summarize=true,
    )
}}
