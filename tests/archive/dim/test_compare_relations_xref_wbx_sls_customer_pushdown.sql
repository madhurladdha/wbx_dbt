-- Output can have different bill_source_customer_code for one trade_type_code, so excluding bill_source_customer_code from comparision
{{ config(enabled=false, severity="warn") }}


{% set a_relation = ref("conv_sls_wtx_cust_pushdown_xref") %}

{% set b_relation = ref("xref_wbx_sls_pushdown_customer") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=["bill_customer_address_guid", "bill_source_customer_code"],
        summarize=false,
    )
}}
