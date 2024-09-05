{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

{{ ent_dbt_package.test_time_travel(
    table_name='dim_wbx_company',
    table_col_1='0',
    table_col_2='0',
    table_col_3='0',
    travel_back_days='1',
    var_pct_threshold='20') 
}}
