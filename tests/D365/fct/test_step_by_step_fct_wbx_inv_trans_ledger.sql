{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 

/* Disabled it for now as stg_f_wbx_inv_trans_ledger is empty*/

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='stg_f_wbx_inv_trans_ledger',
    table_b_name='fct_wbx_inv_trans_ledger',
    table_a_col_1='TRANSACTION_AMT',
    table_a_col_2='TRANSACTION_QTY',
    table_a_col_3='TRANSACTION_UNIT_COST',
    table_b_col_1='TRANSACTION_AMT',
    table_b_col_2='TRANSACTION_QTY',
    table_b_col_3='TRANSACTION_UNIT_COST',
    date_field='gl_date',
    var_pct_threshold='1') 
}}