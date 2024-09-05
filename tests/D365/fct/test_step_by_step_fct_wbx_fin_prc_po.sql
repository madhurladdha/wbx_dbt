{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='stg_f_wbx_fin_prc_po',
    table_b_name='fct_wbx_fin_prc_po',
    table_a_col_1='LINE_TOTAL_AMOUNT',
    table_a_col_2='LINE_ORDER_QUANTITY',
    table_a_col_3='LINE_RECEIVED_AMT',
    table_b_col_1='BASE_ORDER_TOTAL_AMOUNT',
    table_b_col_2='LINE_ORDER_QUANTITY',
    table_b_col_3='BASE_LINE_RECEIVED_AMT',
    date_field='load_date',
    var_pct_threshold='1') 
}}