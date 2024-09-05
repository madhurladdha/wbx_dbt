{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='int_f_wbx_mfg_pct_weekly_pre_agg',
    table_b_name='fct_wbx_mfg_pct_weekly_agg',
    table_a_col_1='SUPPLY_STOCK_ADJ_QTY',
    table_a_col_2='DEMAND_STOCK_ADJ_QTY',
    table_a_col_3='0',
    table_b_col_1='SUPPLY_STOCK_ADJ_QTY',
    table_b_col_2='DEMAND_STOCK_ADJ_QTY',
    table_b_col_3='0',
    date_field='load_date',
    var_pct_threshold='1') 
}}