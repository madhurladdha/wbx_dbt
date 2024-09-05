{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_budget_promo",
        table_col_1="SI_B_VOL_CSE",
        table_col_2="SI_CANNIB_VOL_CSE",
        table_col_3="ONPROMOPHASINGPERCENT_SI",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
