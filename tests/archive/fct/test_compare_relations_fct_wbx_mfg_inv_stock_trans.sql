{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation=ref('conv_inv_wtx_stock_trans_fact') %} 

{% set dbt_relation=ref('fct_wbx_mfg_inv_stock_trans') %} 

{{ ent_dbt_package.compare_relations( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 

    exclude_columns=[
        "item_guid",
        "business_unit_address_guid",
        "load_date",
        "update_date"
    ], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
