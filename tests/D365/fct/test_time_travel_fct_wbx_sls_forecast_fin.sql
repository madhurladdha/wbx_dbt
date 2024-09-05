{{ config(enabled=true, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_forecast_fin",
        table_col_1="TOT_VOL_SP_UL_UOM",
        table_col_2="TOT_VOL_SP_KG_UOM_PRE_ADJUSTMENT",
        table_col_3="RETAIL_TOT_VOL_SP_BASE_UOM",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
