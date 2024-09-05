{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

 select source_item_identifier,case_item_number from {{ ref('stg_d_wbx_item') }}
 minus
 select source_item_identifier,case_item_number from {{ ref('dim_wbx_item') }}