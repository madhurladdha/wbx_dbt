{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_fin_gl_mnthly_acctbal',
    table_col_1='txn_ytd_bal',
    table_col_2='base_ytd_bal',
    table_col_3='0',
    travel_back_days='1',
    var_pct_threshold='5') 
}}