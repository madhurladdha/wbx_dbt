{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_fin_prc_po',
    table_col_1='base_line_unit_cost',
    table_col_2='line_open_quantity',
    table_col_3='base_order_total_amount',
    travel_back_days='1',
    var_pct_threshold='5') 
}}
