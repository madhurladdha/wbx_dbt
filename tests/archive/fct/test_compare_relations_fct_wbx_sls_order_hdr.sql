{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation=ref('conv_sls_wtx_slsorder_hdr_fact') %} 

{% set dbt_relation=ref('fct_wbx_sls_order_hdr') %} 

{{ ent_dbt_package.compare_relations( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 

    exclude_columns=[
        "business_unit_address_guid",
        "ship_customer_addr_number_guid",
        "bill_customer_addr_number_guid",
        "load_date",
        "update_date"
    ], 

    primary_key="UNIQUE_KEY",
    summarize=true 

) }} 
