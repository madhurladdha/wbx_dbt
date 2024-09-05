{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags = ["wbx","sales","sales_actuals","actuals"],

) }} 


{% set old_etl_relation=source('FACTS_FOR_COMPARE','v_wtx_sales_summary') %} 

{% set dbt_relation=ref('v_wtx_sales_summary') %} 

{{ ent_dbt_package.compare_relations( 
    a_relation=old_etl_relation, 
    b_relation=dbt_relation, 
    exclude_columns=["account_guid",
                     "update_date",
                     "load_date",
                     "customer_type_description",
                     "ship_sales_rep_address_number",
                     "ship_shipping_method",
                     "bill_name",
                     "ship_bill_name",
                     "ship_sales_rep_type",
                     "ship_sales_rep_name",
                     "bill_sales_rep_address_number",
                     "bill_shipping_method",
                     "bill_bill_name",
                     "bill_sales_rep_type",
                     "bill_sales_rep_name",
                     "brand_code",
                     "brand_name",
                     "case_upc",
                     "consumer_gtin_number",
                     "consumer_unit_size",
                     "consumer_units_per_case",
                     "consumer_upc",
                     "planner_code"],
    summarize=true

) }} 