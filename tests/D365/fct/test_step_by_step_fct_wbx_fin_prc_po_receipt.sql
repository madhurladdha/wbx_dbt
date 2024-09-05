{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='stg_f_wbx_fin_prc_po_receipt',
    table_b_name='fct_wbx_fin_prc_po_receipt',
    table_a_col_1='RECEIPT_ORDER_QUANTITY',
    table_a_col_2='RECEIPT_OPEN_AMT',
    table_a_col_3='RECEIPT_RECEIVED_AMT',
    table_b_col_1='RECEIPT_ORDER_QUANTITY',
    table_b_col_2='BASE_RECEIPT_OPEN_AMT',
    table_b_col_3='BASE_RECEIPT_RECEIVED_AMT',
    date_field='load_date',
    var_pct_threshold='1') 
}}