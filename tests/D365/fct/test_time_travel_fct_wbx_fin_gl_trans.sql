{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_fin_gl_trans',
    table_col_1='txn_ledger_amt',
    table_col_2='base_ledger_amt',
    table_col_3='quantity',
    travel_back_days='1',
    var_pct_threshold='5') 
}}