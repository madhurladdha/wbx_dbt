{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


select
    trim(source_system) as source_system,
    trim(version_date) as version_date,
    trim(version_number) as version_number,
    trim(source_business_unit_code) as source_business_unit_code,
    trim(business_unit_address_guid) as business_unit_address_guid,
    trim(work_center_code) as work_center_code,
    trim(snapshot_day) as snapshot_day,
    trim(effective_date) as effective_date,
    trim(expiration_date) as expiration_date
from wbx_prod.fact.fct_wbx_mfg_plant_wc_weekday_xref
minus
select
    trim(source_system) as source_system,
    trim(version_date) as version_date,
    trim(version_number) as version_number,
    trim(source_business_unit_code) as source_business_unit_code,
    trim(business_unit_address_guid) as business_unit_address_guid,
    trim(work_center_code) as work_center_code,
    trim(snapshot_day) as snapshot_day,
    trim(effective_date) as effective_date,
    trim(expiration_date) as expiration_date
from {{ ref('fct_wbx_mfg_plant_wc_weekday_xref') }}