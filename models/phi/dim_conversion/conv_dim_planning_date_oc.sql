{{
    config(
        materialized = env_var('DBT_MAT_VIEW'),
    )
}}


with source as (

    select * from {{ source('R_EI_SYSADM', 'dim_planning_date_oc') }} WHERE SOURCE_SYSTEM ='{{env_var("DBT_SOURCE_SYSTEM")}}'

),

renamed as (

    select
        SOURCE_SYSTEM,
PLANNING_WEEK_CODE,
CALENDAR_DATE,
CALENDAR_DAY_OF_WEEK,
LY_DATE,
PY_DATE,
PLANNING_WEEK_NO,
PLANNING_WEEK_START_DT,
PLANNING_WEEK_END_DT,
PLANNING_WEEK_DESC,
PLANNING_WEEK_DESCRLONG,
PLANNING_SUBWEEK_CODE,
SUBWEEK_FLAG,
PLANNING_SUBWEEK_START_DT,
PLANNING_SUBWEEK_END_DT,
PLANNING_SUBWEEK_DESC,
PLANNING_MONTH_NO,
PLANNING_MONTH_CODE,
PLANNING_MONTH_START_DT,
PLANNING_MONTH_END_DT,
PLANNING_QUARTER_NO,
PLANNING_QUARTER_START_DT,
PLANNING_QUARTER_END_DT,
PLANNING_YEAR_NO,
PLANNING_YEAR_START_DT,
PLANNING_YEAR_END_DT,
PLANNING_WEEK_NUMOFDAYS,
PLANNING_MONTH_WEEK,
PLANNING_MONTH_NUMOFDAYS,
PLANNING_QUARTER_CODE,
PLANNING_QUARTER_NUMOFDAYS,
PLANNING_YEAR_NUMOFDAYS,
PLANNING_YEAR_WEEK_NO
      from source

)

select * from renamed