{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 

/*disabled this test for now as int_f_wbx_inv_aging is empty*/

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='int_f_wbx_inv_aging',
    table_b_name='fct_wbx_inv_aging',
    table_a_col_1='ON_HAND_QTY',
    table_a_col_2='ON_HAND_KG_QTY',
    table_a_col_3='0',
    table_b_col_1='ON_HAND_QTY',
    table_b_col_2='ON_HAND_KG_QTY',
    table_b_col_3='0',
    date_field='load_date',
    var_pct_threshold='1') 
}}