{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_budget_terms",
        table_col_1="FIXED_ANNUAL_PAYMENT",
        table_col_2="OTHER_DIRECT_PERC",
        table_col_3="CATEGORY_PAYMENT",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
