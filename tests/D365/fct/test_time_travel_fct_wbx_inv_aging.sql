{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_inv_aging',
    table_col_1='on_hand_qty',
    table_col_2='on_hand_kg_qty',
    table_col_3='0',
    travel_back_days='1',
    var_pct_threshold='5') 
}}
