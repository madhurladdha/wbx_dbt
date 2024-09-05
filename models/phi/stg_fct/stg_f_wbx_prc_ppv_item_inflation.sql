{{
    config(
        tags = ["ppv","procurement","item_inflation","ppv_inflation", "inflation"]
    )
}}

with cte_stg as
(
    select 
        buyer_code, 
        buyer_code_description,
        year,
        month,
	    scenario,
        inflation,
        load_date
    from {{ ref('stg_d_wbx_inflation') }}
    unpivot(inflation for month in (oct, nov, dec, jan,feb,mar,apr,may,jun,jul,aug,sep))
),
cte_dt as 
(
    select 
        distinct fiscal_year,
        substr(calendar_month_name,1,3) month,
        calendar_month_start_dt 
    from {{ ref('src_dim_date') }}   
),
cte_vd as 
(
    select 
        distinct fiscal_year,
        fiscal_year_begin_dt 
    from {{ ref('src_dim_date') }} 
) 
select
	stg.buyer_code,
	stg.buyer_code_description,
	stg.year as inflation_year,
	stg.scenario,
	case 
		when upper(scenario)='LIVE' 
		then trunc(current_date,'MM') 
	else vd.fiscal_year_begin_dt end version_dt,
	to_date(dt.calendar_month_start_dt) as calendar_date,
	round(replace(stg.inflation,'%')/100,4) as inflation ,
	stg.load_date
from cte_stg stg 
inner join cte_dt dt 
on substr(stg.year,1,4)=dt.fiscal_year and
	upper(stg.month)=upper(dt.month)
inner join cte_vd vd
on  substr(stg.year,1,4)=vd.fiscal_year
