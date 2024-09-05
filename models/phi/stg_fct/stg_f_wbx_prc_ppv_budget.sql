{{
    config(
        tags = ["ppv","procurement","budget","ppv_budget"]
    )
}}


with stg as(
    select * from {{ ref('stg_d_wbx_budget') }}
),

month_quantity as
(
select
	source_item_identifier,
	company_code,
	year,
	scenario,
	substr(month,1,position('_',month,1)-1) as month,
	quantity,
	description,
	load_date
from stg
	unpivot(quantity for month in (oct_volume, nov_volume, dec_volume, jan_volume,
	feb_volume, mar_volume, apr_volume, may_volume, jun_volume, jul_volume, aug_volume, sep_volume))
),
month_price as (
select
	source_item_identifier,
	company_code,
	year,
	scenario,
	substr(month,1,position('_',month,1)-1) as month,
	price
from stg
unpivot(price for month in (oct_cost_base, nov_cost_base, dec_cost_base, jan_cost_base, feb_cost_base,
mar_cost_base, apr_cost_base, may_cost_base, jun_cost_base, jul_cost_base, aug_cost_base,
sep_cost_base))),
cte_item as 
(	select
		source_item_identifier,
		item_guid,
		item_type,
		buyer_code,
		primary_uom,
		update_date,
		division,
		rank() over (partition by source_item_identifier order by update_date desc) rnk
	from
		{{ ref('dim_wbx_item') }}
	where
		source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
		and buyer_code is not null
    ),
dim as 
(
select
	distinct source_item_identifier,
	item_guid,
	item_type,
	buyer_code,
	primary_uom,
	division
from cte_item
where
	rnk = 1 ) ,
dt as
(
select
	distinct fiscal_year,
	substr(calendar_month_name,1,3) as month_dd,
	calendar_month_start_dt
from
	{{ ref('src_dim_date') }} 
    ) 

select
	month_quantity.source_item_identifier,
	month_quantity.description,
	month_quantity.company_code,
	date_trunc('month',current_date()) as version_dt,
	month_quantity.year as forcast_year,
	month_quantity.scenario,
	dt.calendar_month_start_dt as calendar_date,
	dim.item_guid,
	dim.item_type,
	dim.buyer_code,
	dim.primary_uom,
	quantity,
	price,
	'GBP' as base_currency,
	month_quantity.load_date
from
	month_quantity
join month_price
on
	(month_quantity.source_item_identifier = month_price.source_item_identifier
		and month_quantity.company_code = month_price.company_code
		and month_quantity.year = month_price.year
		and month_quantity.scenario = month_price.scenario
		and month_quantity.month = month_price.month)
left outer join dim 
on
	month_quantity.source_item_identifier = dim.source_item_identifier
	and month_quantity.company_code = dim.division
inner join dt 
on
	(substr(month_quantity.year,1,4)= dt.fiscal_year
	and upper(month_quantity.month)= upper(dt.month_dd))