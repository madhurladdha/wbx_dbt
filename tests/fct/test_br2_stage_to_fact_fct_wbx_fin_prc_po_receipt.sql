{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 


{{ test_stage_to_fact( 
    table_a_name='stg_f_wbx_fin_prc_po_receipt',
    table_b_name='fct_wbx_fin_prc_po_receipt',
    table_a_col_1='RECEIPT_ORDER_QUANTITY',
    table_a_col_2='RECEIPT_OPEN_AMT',
    table_a_col_3='RECEIPT_RECEIVED_AMT',
    table_b_col_1='RECEIPT_ORDER_QUANTITY',
    table_b_col_2='BASE_RECEIPT_OPEN_AMT',
    table_b_col_3='BASE_RECEIPT_RECEIVED_AMT',
    filter_field_a='po_order_company',
    filter_condition_a=('WBX','RFL','IBE'),
    and_condition_a='1=1',
    filter_field_b='po_order_company',
    filter_condition_b=('WBX','RFL','IBE'),
    and_condition_b= "(source_legacy='D365')",
    var_pct_threshold='1') 
}}

/*This test model is comparing stg data with Fact data */