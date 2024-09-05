{{ config( 
    enabled=true,
    severity = 'warn',
    warn_if = '>1'  
) }} 

 select source_system_address_number,business_unit_name,plan_company from {{ ref('stg_d_wbx_plant_dc') }}
 minus
 select source_business_unit_code,business_unit_name,company_code   from {{ ref('dim_wbx_plant_dc') }}