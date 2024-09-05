{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='stg_f_wbx_mfg_supply_sched_dly',
    table_b_name='fct_wbx_mfg_supply_sched_dly',
    table_a_col_1='TRANSACTION_QUANTITY',
    table_a_col_2='0',
    table_a_col_3='0',
    table_b_col_1='TRANSACTION_QUANTITY',
    table_b_col_2='0',
    table_b_col_3='0',
    date_field='snapshot_date',
    var_pct_threshold='1') 
}}