{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


select
    trim(source_business_unit_code) as source_business_unit_code,
    trim(business_unit_address_guid) as business_unit_address_guid,
    trim(work_center_code) as work_center_code,
    trim(ctp_target) as ctp_target,
    trim(ptp_target) as ptp_target,
    trim(unique_key) as unique_key
from wbx_prod.fact.fct_wbx_mfg_plant_ctp_tgt
minus
select
    trim(source_business_unit_code) as source_business_unit_code,
    trim(business_unit_address_guid) as business_unit_address_guid,
    trim(work_center_code) as work_center_code,
    trim(ctp_target) as ctp_target,
    trim(ptp_target) as ptp_target,
    trim(unique_key) as unique_key
from {{ ref('fct_wbx_mfg_plant_ctp_tgt') }}