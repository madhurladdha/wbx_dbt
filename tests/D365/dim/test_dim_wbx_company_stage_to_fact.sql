{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

 select SOURCE_COMPANY_NAME,SOURCE_SYSTEM_ADDRESS_NUMBER from {{ ref('stg_d_wbx_company') }}
 minus
 select COMPANY_NAME,COMPANY_CODE  from {{ ref('dim_wbx_company') }}