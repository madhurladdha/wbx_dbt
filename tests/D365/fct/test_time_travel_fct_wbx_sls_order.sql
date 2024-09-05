{{ config(enabled=true, severity="warn", warn_if=">1") }}

/* change threshold to 20% */

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_order",
        table_col_1="ORDERED_CA_QUANTITY",
        table_col_2="SHIPPED_CA_QUANTITY",
        table_col_3="BASE_RPT_GRS_PRIM_AMT",
        travel_back_days=1,
        var_pct_threshold="20",
    )
}}
