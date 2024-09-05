{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_promo",
        table_col_1="si_b_vol_kg",
        table_col_2="si_a_vol_kg",
        table_col_3="si_t_vol_kg",
        travel_back_days=0,
        var_pct_threshold="3",
    )
}}
