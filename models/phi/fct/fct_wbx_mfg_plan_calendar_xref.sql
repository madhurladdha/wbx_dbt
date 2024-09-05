{{
  config( 
    materialized=env_var('DBT_MAT_TABLE'), 
    tags=["manufacturing", "manufacturing_plan","manufacturing_plan_Calendar"],
    unique_key="UNIQUE_KEY",
    on_schema_change='sync_all_columns'
    )
}}

with cte_fct as 
(
select
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,

    cast(substring(planning_calendar_name,1,255) as text(255) ) as planning_calendar_name  ,

    cast(substring(week_description,1,255) as text(255) ) as week_description  ,

    cast(week_start_date as date) as week_start_date  ,

    cast(week_end_date as date) as week_end_date  ,

    cast(open_days as number(38,0) ) as open_days  ,

    cast(load_date as date) as load_date  ,

    cast(update_date as date) as update_date

from {{ ref('stg_f_wbx_mfg_plan_calendar_xref') }}
),
cte_final as 
(
    select 
    a.*,
    {{ dbt_utils.surrogate_key([
        "source_system",
        "planning_calendar_name",
        "week_description"
        ]) }} as unique_key
    from cte_fct a 
)
select * from cte_final
qualify row_number() over (partition by unique_key order by unique_key)=1