{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

 select SOURCE_SYSTEM_ADDRESS_NUMBER,SUPPLIER_NAME from {{ ref('stg_d_wbx_supplier') }}
 minus
 select SOURCE_SYSTEM_ADDRESS_NUMBER,SUPPLIER_NAME   from {{ ref('dim_wbx_supplier') }}