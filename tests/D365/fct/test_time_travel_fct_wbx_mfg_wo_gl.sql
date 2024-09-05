{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_mfg_wo_gl",
        table_col_1="TRANSACTION_QUANTITY",
        table_col_2="0",
        table_col_3="0",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
