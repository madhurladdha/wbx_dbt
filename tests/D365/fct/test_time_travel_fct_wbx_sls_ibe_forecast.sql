{{ config(enabled=true, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_ibe_forecast",
        table_col_1="AP_TOTAL_TRADE",
        table_col_2="TOT_VOL_CA",
        table_col_3="AP_TOT_PRIME_COST_STANDARD_BOUGHT_IN",
        travel_back_days=1,
        var_pct_threshold="20",
    )
}}
