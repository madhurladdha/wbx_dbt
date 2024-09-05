{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["wbx","manufacturing","demand","agg"]

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_mfg_wtx_demand_comp_agg') %} 

{% set dbt_relation=ref('v_mfg_wtx_demand_comp_agg') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["WO_SRC_ITEM_GUID","COMP_SRC_ITEM_GUID","WO_VENDOR_ADDRESS_GUID","COMP_VENDOR_ADDRESS_GUID"],
    summarize=false

) }} 