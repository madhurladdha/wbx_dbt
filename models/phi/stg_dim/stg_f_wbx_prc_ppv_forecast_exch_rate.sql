{{
    config(
        tags = ["ppv","procurement","forecast_exchange_rate"]
    )
}}

with cte_stg as
(
    select 
        year,
        month,
        exchange_rate,
        scenario,
        load_date
	from {{ ref('stg_d_wbx_forecast_exch_rate') }}
	unpivot(exchange_rate for month in (OCT,NOV,DEC,JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP))
),
cte_dt as 
(
    select 
        distinct fiscal_year,
        substr(calendar_month_name,1,3) month,
        calendar_month_start_dt
    from {{ ref('src_dim_date')}}
),
cte_vd as 
(
    select
        distinct fiscal_year,
        fiscal_year_begin_dt
    from {{ ref('src_dim_date')}}    
)
select 
    stg.year as exch_rate_year,
	stg.scenario as scenario,
	case 
        when upper(scenario)='LIVE' then trunc(current_date,'MM') 
    else vd.fiscal_year_begin_dt end as version_dt,
	to_date(dt.calendar_month_start_dt) as calendar_date,
	stg.exchange_rate,
	stg.load_date
from cte_stg stg 
inner join cte_dt dt
on substr(stg.year,1,4)=dt.fiscal_year and
    upper(stg.month)=upper(dt.month) 
inner join  cte_vd vd
on substr(stg.year,1,4)=vd.fiscal_year 
