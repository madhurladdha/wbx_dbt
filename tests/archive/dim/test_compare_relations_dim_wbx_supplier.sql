{{ config( 

  enabled=false, 

  severity = 'warn'
) }} 


{% set a_relation=ref('conv_adr_supplier_master_dim')%}

{% set b_relation=ref('dim_wbx_supplier') %}

{{ ent_dbt_package.compare_relations(
    a_relation=a_relation,
    b_relation=b_relation,
    exclude_columns=["DATE_UPDATED","DATE_INSERTED","VOUCHER_DATE","REPORTING_1099","AP_ACCOUNT","VENDOR_SERIVICE","VENDOR_SPECIALTY","ALSO_A_CUSTOMER"],
    primary_key='UNIQUE_KEY',
    summarize=false
) }}