{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ test_stage_to_fact( 
    table_a_name='stg_f_wbx_sls_order',
    table_b_name='fct_wbx_sls_order',
    table_a_col_1='sales_tran_quantity',
    table_a_col_2='trans_lineamount_confirmed',
    table_a_col_3='trans_rpt_grs_amt',
    table_b_col_1='sales_tran_quantity',
    table_b_col_2='trans_lineamount_confirmed',
    table_b_col_3='trans_rpt_grs_amt',
    filter_field_a='sales_order_company',
    filter_condition_a=('WBX','RFL','IBE'),
    and_condition_a='1=1',
    filter_field_b='sales_order_company',
    filter_condition_b=('WBX','RFL','IBE'),
    and_condition_b= "(source_legacy='D365')",
    var_pct_threshold='5') 
}}

/*This test model is comparing stg data with Fact data */