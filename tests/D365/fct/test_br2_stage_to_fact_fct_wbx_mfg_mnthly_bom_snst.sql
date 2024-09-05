{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ test_stage_to_fact( 
    table_a_name='int_f_wbx_mfg_monthly_bom_snapshot',
    table_b_name='fct_wbx_mfg_monthly_bom_snapshot',
    table_a_col_1='comp_required_qty',
    table_a_col_2='comp_perfection_qty',
    table_a_col_3='root_qty',
    table_b_col_1='comp_required_qty',
    table_b_col_2='comp_perfection_qty',
    table_b_col_3='root_qty',
    filter_field_a='root_company_code',
    filter_condition_a=('WBX','RFL','IBE'),
    and_condition_a='1=1',
    filter_field_b='root_company_code',
    filter_condition_b=('WBX','RFL','IBE'),
    and_condition_b= "(source_legacy='D365') and bom_flag='D'",
    var_pct_threshold='5') 
}}

/*This test model is comparing stg data with Fact data */