{{
    config(
        materialized=env_var("DBT_MAT_TABLE"),
        tags=env_var("DBT_TAGS","manufacturing"),
        transient=false,
        on_schema_change="sync_all_columns"
    )
}}

with src as (
select          
        plant,
        work_center_name as WORK_CENTER_CODE,
        UPPER(snapshot_day) as snapshot_day,
        to_date(Effective_Date,'MM/DD/YYYY' ) as effective_date,
        to_date(Expiration_Date,'MM/DD/YYYY' ) as expiration_date
from {{ ref("src_ctp_plant_wc_day") }}
order by plant asc, work_center_name asc, snapshot_day asc
)
, dim_date as (
select UPPER(calendar_day_of_week) as calendar_day_of_week
from {{ ref('dim_wbx_date_oc') }}
group by calendar_day_of_week
)
, expr as (
select
LAG(plant) OVER (ORDER BY plant asc, WORK_CENTER_CODE asc, snapshot_day asc) AS prev_plant,
LAG(WORK_CENTER_CODE) OVER (ORDER BY plant asc, WORK_CENTER_CODE asc, snapshot_day asc) AS prev_WORK_CENTER_CODE,
LAG(snapshot_day) OVER (ORDER BY plant asc, WORK_CENTER_CODE asc, snapshot_day asc) AS prev_snapshot_day,
case when Plant=prev_plant and Work_Center_code=prev_WORK_CENTER_CODE and Snapshot_Day=prev_snapshot_day then 1 else 0 end cnt,
--case when Plant=prev_plant and Work_Center_code=prev_WORK_CENTER_CODE and Snapshot_Day=prev_snapshot_day then cnt1+1 else 1 end cnt,
plant,
WORK_CENTER_CODE,
snapshot_day,
effective_date,
expiration_date
from src
)
, final as (
select 
plant as source_business_unit_code,
WORK_CENTER_CODE,
snapshot_day,
effective_date,
expiration_date,
case when dim_date.calendar_day_of_week is null then 1 
        else case when cnt >= 1 then 2 
                else 0 end 
        end as ERROR_FLAG,
case when ERROR_FLAG=1 then 'INCORRECT DAY FORMAT' 
        else case when ERROR_FLAG=2 then 'OVERLAPPING EFFECTIVE DATES' 
                else 'NO ERROR' end 
        end as ERROR_MSG,
Systimestamp() as LOAD_DATE,
Systimestamp() as update_date
from expr
left join dim_date
on dim_date.calendar_day_of_week = expr.snapshot_day
)
select 
    cast(substring(source_business_unit_code,1,255) as text(255) ) as source_business_unit_code  ,

    cast(substring(work_center_code,1,255) as text(255) ) as work_center_code  ,

    cast(substring(snapshot_day,1,255) as text(255) ) as snapshot_day  ,

    cast(effective_date as date) as effective_date  ,

    cast(expiration_date as date) as expiration_date  ,

    cast(load_date as timestamp_ntz(9) ) as load_date  ,

    cast(update_date as timestamp_ntz(9) ) as update_date  ,

    cast(error_flag as number(38,0) ) as error_flag  ,

    cast(substring(error_msg,1,255) as text(255) ) as error_msg 
from final