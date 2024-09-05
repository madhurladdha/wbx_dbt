{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='fct_wbx_fin_prc_po_receipt',
    table_col_1='base_receipt_open_amt',
    table_col_2='base_receipt_received_amt',
    table_col_3='receipt_received_quantity',
    travel_back_days='1',
    var_pct_threshold='5') 
}}
