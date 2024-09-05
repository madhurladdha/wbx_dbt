{{ config(enabled=false, severity="warn", warn_if=">1") }}

{{
    ent_dbt_package.test_time_travel(
        table_name="fct_wbx_sls_terms",
        table_col_1="FIXED_ANNUAL_PAYMENT",
        table_col_2="CATEGORY_PAYMENT",
        table_col_3="FIELD_MARKETING",
        travel_back_days=1,
        var_pct_threshold="5",
    )
}}
