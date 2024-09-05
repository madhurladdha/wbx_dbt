{{
    config(
    on_schema_change='sync_all_columns'
    )
}}
-----not using pre hook to delete records(as in IICS) because it is truncating all records where source in wbx fromm dim_planning_date_oc table.

with STG as
(
    select * from {{ref('stg_d_wbx_planning_date_oc')}}
),

HIST as
(
    select * from {{ref('conv_dim_planning_date_oc') }} where 1=2
),

FINAL as 
(
    select * from STG
   
)

select
  cast(substring(source_system,1,255) as text(255) ) as source_system  ,
    cast(substring(planning_week_code,1,10) as text(10) ) as planning_week_code  ,
    cast(calendar_date as timestamp_ntz(9) ) as calendar_date  ,
    cast(substring(calendar_day_of_week,1,10) as text(10) ) as calendar_day_of_week  ,
    cast(ly_date as timestamp_ntz(9) ) as ly_date  ,
    cast(py_date as timestamp_ntz(9) ) as py_date  ,
    cast(planning_week_no as number(2,0) ) as planning_week_no  ,
    cast(planning_week_start_dt as timestamp_ntz(9) ) as planning_week_start_dt  ,
    cast(planning_week_end_dt as timestamp_ntz(9) ) as planning_week_end_dt  ,
    cast(substring(planning_week_desc,1,10) as text(10) ) as planning_week_desc  ,
    cast(substring(planning_week_descrlong,1,50) as text(50) ) as planning_week_descrlong  ,
    cast(substring(planning_subweek_code,1,20) as text(20) ) as planning_subweek_code  ,
    cast(substring(subweek_flag,1,1) as text(1) ) as subweek_flag  ,
    cast(planning_subweek_start_dt as timestamp_ntz(9) ) as planning_subweek_start_dt  ,
    cast(planning_subweek_end_dt as timestamp_ntz(9) ) as planning_subweek_end_dt  ,
    cast(substring(planning_subweek_desc,1,20) as text(20) ) as planning_subweek_desc  ,
    cast(planning_month_no as number(2,0) ) as planning_month_no  ,
    cast(substring(planning_month_code,1,10) as text(10) ) as planning_month_code  ,
    cast(planning_month_start_dt as timestamp_ntz(9) ) as planning_month_start_dt  ,
    cast(planning_month_end_dt as timestamp_ntz(9) ) as planning_month_end_dt  ,
    cast(planning_quarter_no as number(2,0) ) as planning_quarter_no  ,
    cast(planning_quarter_start_dt as timestamp_ntz(9) ) as planning_quarter_start_dt  ,
    cast(planning_quarter_end_dt as timestamp_ntz(9) ) as planning_quarter_end_dt  ,
    cast(planning_year_no as number(4,0) ) as planning_year_no  ,
    cast(planning_year_start_dt as timestamp_ntz(9) ) as planning_year_start_dt  ,
    cast(planning_year_end_dt as timestamp_ntz(9) ) as planning_year_end_dt  ,
    cast(planning_week_numofdays as number(38,0) ) as planning_week_numofdays  ,
    cast(planning_month_week as number(38,0) ) as planning_month_week  ,
    cast(planning_month_numofdays as number(38,0) ) as planning_month_numofdays  ,
    cast(planning_quarter_code as number(38,0) ) as planning_quarter_code  ,
    cast(planning_quarter_numofdays as number(38,0) ) as planning_quarter_numofdays  ,
    cast(planning_year_numofdays as number(38,0) ) as planning_year_numofdays  ,
    cast(planning_year_week_no as number(38,0) ) as planning_year_week_no 

From FINAL