with STG as (
    select * from {{ref('stg_d_wbx_date_oc')}}
),

History as (
    select * from {{ref('conv_dim_date_oc')}} where 1=2      ----not fetching any data from history as it's a truncate load table
                                                             -----using conv model just for test purpose
)

select
cast(fiscal_date_id as number(38,0) ) as fiscal_date_id  ,
    cast(fiscal_period_no as number(38,0) ) as fiscal_period_no  ,
    cast(fiscal_year_period_no as number(38,0) ) as fiscal_year_period_no  ,
    cast(substring(fiscal_period_desc,1,20) as text(20) ) as fiscal_period_desc  ,
    cast(fiscal_period_begin_dt as timestamp_ntz(9) ) as fiscal_period_begin_dt  ,
    cast(fiscal_period_end_dt as timestamp_ntz(9) ) as fiscal_period_end_dt  ,
    cast(fiscal_year_quarter_no as number(38,0) ) as fiscal_year_quarter_no  ,
    cast(substring(fiscal_quarter_desc,1,20) as text(20) ) as fiscal_quarter_desc  ,
    cast(fiscal_quarter_start_dt as timestamp_ntz(9) ) as fiscal_quarter_start_dt  ,
    cast(fiscal_quarter_end_dt as timestamp_ntz(9) ) as fiscal_quarter_end_dt  ,
    cast(substring(fiscal_year,1,4) as text(4) ) as fiscal_year  ,
    cast(fiscal_year_begin_dt as timestamp_ntz(9) ) as fiscal_year_begin_dt  ,
    cast(fiscal_year_end_dt as timestamp_ntz(9) ) as fiscal_year_end_dt  ,
    cast(fiscal_year_week_no as number(38,0) ) as fiscal_year_week_no  ,
    cast(calendar_date_id as number(38,0) ) as calendar_date_id  ,
    cast(calendar_date as timestamp_ntz(9) ) as calendar_date  ,
    cast(substring(calendar_day_of_week,1,20) as text(20) ) as calendar_day_of_week  ,
    cast(calendar_year as number(38,10) ) as calendar_year  ,
    cast(calendar_year_begin_dt as timestamp_ntz(9) ) as calendar_year_begin_dt  ,
    cast(calendar_year_end_dt as timestamp_ntz(9) ) as calendar_year_end_dt  ,
    cast(calendar_year_quarter_no as number(38,0) ) as calendar_year_quarter_no  ,
    cast(substring(calendar_quarter_desc,1,20) as text(20) ) as calendar_quarter_desc  ,
    cast(calendar_quarter_start_dt as timestamp_ntz(9) ) as calendar_quarter_start_dt  ,
    cast(calendar_quarter_end_dt as timestamp_ntz(9) ) as calendar_quarter_end_dt  ,
    cast(calendar_year_month_no as number(38,0) ) as calendar_year_month_no  ,
    cast(calendar_month_no as number(38,0) ) as calendar_month_no  ,
    cast(substring(calendar_month_name,1,20) as text(20) ) as calendar_month_name  ,
    cast(substring(calendar_month_desc,1,20) as text(20) ) as calendar_month_desc  ,
    cast(calendar_month_start_dt as timestamp_ntz(9) ) as calendar_month_start_dt  ,
    cast(calendar_month_end_dt as timestamp_ntz(9) ) as calendar_month_end_dt  ,
    cast(calendar_year_week_no as number(38,0) ) as calendar_year_week_no  ,
    cast(calendar_week_begin_dt as timestamp_ntz(9) ) as calendar_week_begin_dt  ,
    cast(calendar_week_end_dt as timestamp_ntz(9) ) as calendar_week_end_dt  ,
    cast(substring(calendar_business_day_flag,1,1) as text(1) ) as calendar_business_day_flag  ,
    cast(substring(source_ind,1,30) as text(30) ) as source_ind  ,
    cast(load_date as timestamp_ntz(9) ) as load_date  ,
    cast(update_date as timestamp_ntz(9) ) as update_date  ,
    cast(report_fiscal_year_period_no as number(38,0) ) as report_fiscal_year_period_no  ,
    cast(report_fiscal_year as number(38,0) ) as report_fiscal_year  ,
    cast(substring(source_system,1,255) as text(255) ) as source_system  ,
    cast(substring(oc_period_name,1,255) as text(255) ) as oc_period_name  ,
    cast(oc_fiscal_year as number(38,0) ) as oc_fiscal_year  ,
    cast(oc_fiscal_period_no as number(38,0) ) as oc_fiscal_period_no  ,
    cast(oc_fiscal_year_period_no as number(38,0) ) as oc_fiscal_year_period_no  ,
    cast(substring(oc_fiscal_period_desc,1,20) as text(20) ) as oc_fiscal_period_desc  ,
    cast(oc_fiscal_period_begin_dt as timestamp_ntz(9) ) as oc_fiscal_period_begin_dt  ,
    cast(oc_fiscal_period_end_dt as timestamp_ntz(9) ) as oc_fiscal_period_end_dt  ,
    cast(substring(uk_holiday_flag,1,10) as text(10) ) as uk_holiday_flag  ,
    cast(day_of_month_business as number(10,0) ) as day_of_month_business  ,
    cast(day_of_month_actual as number(10,0) ) as day_of_month_actual 

    From STG