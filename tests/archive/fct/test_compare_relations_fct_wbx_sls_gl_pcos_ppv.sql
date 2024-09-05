{{ config(enabled=false, severity="warn", warn_if=">1") }}

{% set old_etl_relation = ref("conv_sls_wtx_gl_pcos_ppv_fact") %}

{% set dbt_relation = ref("fct_wbx_sls_gl_pcos_ppv") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "account_guid",
            "business_unit_address_guid",
            "item_guid",
            "ship_customer_addr_number_guid",
            "bill_customer_addr_number_guid"
        ],
        summarize=false,
        primary_key="UNIQUE_KEY"
    )
}}
