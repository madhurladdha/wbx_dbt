{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='stg_f_wbx_sls_order',
    table_b_name='fct_wbx_sls_order',
    table_a_col_1='sales_tran_quantity',
    table_a_col_2='trans_lineamount_confirmed',
    table_a_col_3='trans_rpt_grs_amt',
    table_b_col_1='sales_tran_quantity',
    table_b_col_2='trans_lineamount_confirmed',
    table_b_col_3='trans_rpt_grs_amt',
    date_field='load_date',
    var_pct_threshold='5') 
}}