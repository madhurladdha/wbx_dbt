{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_fin_ap_voucher_hdr',
    table_col_1='txn_gross_amt',
    table_col_2='txn_open_amt',
    table_col_3='0',
    travel_back_days='1',
    var_pct_threshold='5') 
}}
