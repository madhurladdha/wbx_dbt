{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_inv_trans_ledger",
        table_col_1="TRANSACTION_AMT",
        table_col_2="TRANSACTION_QTY",
        table_col_3="TRANSACTION_UNIT_COST",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
