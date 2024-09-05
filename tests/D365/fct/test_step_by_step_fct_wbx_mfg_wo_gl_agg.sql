{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_step_by_step( 
    table_a_name='int_f_wbx_mfg_wo_gl_agg',
    table_b_name='fct_wbx_mfg_wo_gl_agg',
    table_a_col_1='A510045_GL_AMOUNT',
    table_a_col_2='A718060_GL_AMOUNT',
    table_a_col_3='PRODUCED_QTY',
    table_b_col_1='A510045_GL_AMOUNT',
    table_b_col_2='A718060_GL_AMOUNT',
    table_b_col_3='PRODUCED_QTY',
    date_field='load_date',
    var_pct_threshold='1') 
}}