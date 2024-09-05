{{ config(enabled=true, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_budget",
        table_col_1="tot_vol_ul",
        table_col_2="ap_total_trade",
        table_col_3="ap_net_sales_value",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
