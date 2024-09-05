{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_budget_fin",
        table_col_1="BUDGET_QTY_KG",
        table_col_2="GROSS_VALUE_AMT",
        table_col_3="NET_VALUE_AMT",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
