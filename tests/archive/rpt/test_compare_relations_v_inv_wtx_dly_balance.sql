{{ config( 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  tags=["inventory", "trans_ledger","inv_daily_balance","inv_aging"],
  enabled=false

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_inv_wtx_dly_balance') %} 

{% set dbt_relation=ref('v_inv_wtx_dly_balance') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["item_guid",
                     "business_unit_address_guid",
                     "location_guid",
                     "lot_guid",
                     "case_upc",
                     "load_date",    
                     "update_date",      
                     "source_updated_d_id"
                    ],
    summarize=true

) }} 