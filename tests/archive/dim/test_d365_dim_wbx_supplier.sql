{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


{% set old_etl_relation = ref('dim_wbx_supplier') %} 

{% set dbt_relation= 'wbx_prod.dim.dim_wbx_supplier'  %} 

{% set filter_field= "1" %} 

{% set filter_values= "'1'"  %} 



{{ compare_relations_d365( 

    a_relation=old_etl_relation, 

    b_relation=dbt_relation, 
    c_filter_field = filter_field,
    d_filter_values = filter_values,

    exclude_columns=["DATE_UPDATED","DATE_INSERTED","VOUCHER_DATE","REPORTING_1099","AP_ACCOUNT","VENDOR_SERIVICE","VENDOR_SPECIALTY","ALSO_A_CUSTOMER"], 

    primary_key="UNIQUE_KEY",
    summarize=false 

) }} 
