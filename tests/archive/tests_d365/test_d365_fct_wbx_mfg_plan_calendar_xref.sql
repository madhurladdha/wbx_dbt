{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


select
    trim(source_system) as source_system,
    trim(planning_calendar_name) as planning_calendar_name,
    trim(week_description) as week_description,
    trim(week_start_date) as week_start_date,
    trim(week_end_date) as week_end_date,
    trim(open_days) as open_days,
    trim(unique_key) as unique_key
from wbx_prod.fact.fct_wbx_mfg_plan_calendar_xref
minus
select
    trim(source_system) as source_system,
    trim(planning_calendar_name) as planning_calendar_name,
    trim(week_description) as week_description,
    trim(week_start_date) as week_start_date,
    trim(week_end_date) as week_end_date,
    trim(open_days) as open_days,
    trim(unique_key) as unique_key
from {{ ref('fct_wbx_mfg_plan_calendar_xref') }}