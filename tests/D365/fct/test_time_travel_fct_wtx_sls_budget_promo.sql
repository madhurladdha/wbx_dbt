{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wtx_sls_budget_promo",
        table_col_1="ACTUALS_TOT_VOL_KG",
        table_col_2="ACTUALS_AP_NET_NET_SALES_VALUE",
        table_col_3="ACTUALS_AP_TOTAL_TRADE_CUST_INVOICED",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
