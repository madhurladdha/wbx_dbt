{{ config(enabled=false, severity="warn") }}

{% set a_relation = ref("conv_fct_wbx_mfg_inv_stock_adj") %}

{% set b_relation = ref("fct_wbx_mfg_inv_stock_adj") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=[
            "business_unit_address_guid",
            "item_guid",
            "load_date",
            "update_date",
        ],
        primary_key="unique_key",
        summarize=false,
    )
}}