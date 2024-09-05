{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_fin_ap_payment_dtl',
    table_col_1='base_payment_amt',
    table_col_2='pcomp_payment_amt',
    table_col_3='phi_payment_amt',
    travel_back_days='1',
    var_pct_threshold='5') 
}}
