{{ config(enabled=false, severity="warn", warn_if=">1") }}
--time travel is not enables for fct_wbx_sls_epos
{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_epos",
        table_col_1="qty_ca",
        table_col_2="qty_kg",
        table_col_3="qty_ul",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
