{{ config(enabled=false, severity="warn", warn_if=">1") }}

{% set old_etl_relation = ref("conv_v_sls_wtx_slsorder_alloc_basis_trade_type") %}

{% set dbt_relation = ref("int_f_wbx_sls_order_allocbasis_tradetype") %}


{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "LOAD_DATE",
            "UPDATE_DATE",
            "item_guid",
        ],
        summarize=false,
    )
}}
