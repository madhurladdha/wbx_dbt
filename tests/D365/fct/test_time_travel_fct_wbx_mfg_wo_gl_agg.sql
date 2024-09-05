{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_mfg_wo_gl_agg",
        table_col_1="A510045_GL_AMOUNT",
        table_col_2="A550010_GL_AMOUNT",
        table_col_3="0",
        travel_back_days=1,
        var_pct_threshold="20",
    )
}}
