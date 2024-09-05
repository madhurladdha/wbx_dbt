{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='int_f_wbx_sls_ibe_forecast',
    table_b_name='fct_wbx_sls_ibe_forecast',
    table_a_col_1='AP_TOTAL_TRADE',
    table_a_col_2='TOT_VOL_CA',
    table_a_col_3='AP_TOT_PRIME_COST_STANDARD_BOUGHT_IN',
    table_b_col_1='AP_TOTAL_TRADE',
    table_b_col_2='TOT_VOL_CA',
    table_b_col_3='AP_TOT_PRIME_COST_STANDARD_BOUGHT_IN',
    date_field='snapshot_date',
    var_pct_threshold='1') 
}}