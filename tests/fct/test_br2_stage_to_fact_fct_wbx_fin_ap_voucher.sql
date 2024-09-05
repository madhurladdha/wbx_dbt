{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ test_stage_to_fact( 
    table_a_name='stg_f_wbx_fin_ap_voucher',
    table_b_name='fct_wbx_fin_ap_voucher',
    table_a_col_1='txn_gross_amt',
    table_a_col_2=0,
    table_a_col_3=0,
    table_b_col_1='txn_gross_amt',
    table_b_col_2=0,
    table_b_col_3=0,
    filter_field_a='document_company',
    filter_condition_a=('WBX','RFL','IBE'),
    and_condition_a='1=1',
    filter_field_b='document_company',
    filter_condition_b=('WBX','RFL','IBE'),
    and_condition_b= "(source_legacy='D365')",
    var_pct_threshold='5') 
}}

/*This test model is comparing stg data with Fact data */