{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

 select SOURCE_ITEM_IDENTIFIER,SOURCE_BUSINESS_UNIT_CODE from {{ ref('stg_d_wbx_item_ext') }}
 minus
 select SOURCE_ITEM_IDENTIFIER,SOURCE_BUSINESS_UNIT_CODE  from {{ ref('dim_wbx_item_ext') }}