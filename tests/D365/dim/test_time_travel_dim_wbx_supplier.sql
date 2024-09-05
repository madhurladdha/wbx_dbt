{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 

/*disabled this test, since time travel is not applicable on this*/

{{ ent_dbt_package.test_time_travel(
    table_name='dim_wbx_supplier',
    table_col_1='0',
    table_col_2='0',
    table_col_3='0',
    travel_back_days='1',
    var_pct_threshold='20') 
}}
