{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_inv_daily_balance',
    table_col_1='on_hand_qty',
    table_col_2='purchase_order_qty',
    table_col_3='0',
    travel_back_days='1',
    var_pct_threshold='5') 
}}
