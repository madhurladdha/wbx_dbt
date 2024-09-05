{{ config( 
  enabled=false, 
  severity = 'warn',
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('v_inv_wtx_dly_balance') %} 

{% set dbt_relation= 'wbx_prod.r_ei_sysadm.v_inv_wtx_dly_balance'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,
    exclude_columns=["load_date","update_date","SOURCE_UPDATED_D_ID","shelf_life_days","lot_age_days","lot_expiration_date","lot_sellby_date","BUSINESS_UNIT_NAME"],

    primary_key=["SOURCE_ITEM_IDENTIFIER","INVENTORY_SNAPSHOT_DATE","SOURCE_BUSINESS_UNIT_CODE","SOURCE_LOT_CODE","source_location_code"],
    summarize=true 

)
}}

/*exclyded  few fields as these values are highly volatile and  keeps changing on daily basis*/