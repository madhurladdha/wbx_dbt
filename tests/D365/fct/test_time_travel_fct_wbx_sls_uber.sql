{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_uber",
        table_col_1="LY_TRANS_RPT_NET_PRICE",
        table_col_2="LY_BASE_EXT_BOUGHTIN_AMT",
        table_col_3="CY_GL_PHI_PERMD_CSH_DISC",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
