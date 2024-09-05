{{
    config(
    materialized = env_var('DBT_MAT_VIEW'),
    )
}}

WITH old_fct AS 
(
    SELECT * FROM {{source('FACTS_FOR_COMPARE','mfg_wtx_plan_calendar_xref')}} 
),
converted_fct as (
    SELECT  
           cast(substring(source_system,1,255) as text(255) ) as source_system  ,

            cast(substring(planning_calendar_name,1,255) as text(255) ) as planning_calendar_name  ,

            cast(substring(week_description,1,255) as text(255) ) as week_description  ,

            cast(week_start_date as date) as week_start_date  ,

            cast(week_end_date as date) as week_end_date  ,

            cast(open_days as number(38,0) ) as open_days  ,

            cast(load_date as date) as load_date  ,

            cast(update_date as date) as update_date
    FROM old_fct
)

Select a.*, {{ dbt_utils.surrogate_key([
        "source_system",
        "planning_calendar_name",
        "week_description"
        ]) }} as unique_key
from converted_fct a


