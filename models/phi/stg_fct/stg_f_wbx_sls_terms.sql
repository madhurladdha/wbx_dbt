{{ config(tags=["sales", "terms","sls_terms"]) }}

{% set now = modules.datetime.datetime.now() %}
{%- set full_load_day -%} {{env_var('DBT_FULL_LOAD_DAY')}} {%- endset -%}
{%- set day_today -%} {{ now.strftime('%A') }} {%- endset -%}

with rob_lumpsum_spread_custskuday as (
    select * from {{ ref('src_exc_fact_rob_lumpsum_spread_custskuday')}}
),
customer as (
    select * from {{ref('src_exc_dim_pc_customer')}}
),
product as (
    select * from {{ ref('src_exc_dim_pc_product')}}
),
rob as (
    select * from {{ ref('src_exc_dim_rob')}}
),
users as (
    select * from {{ ref('src_exc_dim_users')}}
),
rob_dates as (
    select * from {{ ref('src_exc_fact_rob_dates')}}
),
fact_rob_status as (
    select * from {{ ref('src_exc_fact_rob_status')}} 
),
dim_rob_statuses as (
    select * from {{ ref('src_exc_dim_rob_statuses')}}
),
rob_status as (
    select a.rob_idx, b.status_code, b.status_name, b.verb  
    from fact_rob_status a
	inner join dim_rob_statuses b    
	on a.status_idx=b.status_idx
),
dim_rob_impactoption as (
    select * from {{ ref('src_exc_dim_rob_impactoption')}}
),
dim_rob_impact as (
  select * from {{ ref('src_exc_dim_rob_impact')}}
),
fact_rob_impactoption as (
select * from {{ ref('src_exc_fact_rob_impactoption')}}
),
rob_impactoption as (
    select a.rob_idx, a.impactoption_idx as io_idx, b.impactoption_code, 
    b.impactoption_name, c.impact_code, c.impact_name,
    b.islumpsumtype, a.value, a.financialimpactestimate
    from fact_rob_impactoption a
    inner join dim_rob_impactoption b     
    on a.impactoption_idx=b.impactoption_idx
    inner join dim_rob_impact c
    on b.impact_idx=c.impact_idx
),
rob_impactoption1 as (
    select a.rob_idx, b.impactoption_idx, b.impactoption_code, 
    b.impactoption_name, c.impact_code, c.impact_name
	, b.islumpsumtype, a.value, a.financialimpactestimate
            from fact_rob_impactoption a
            inner join dim_rob_impactoption b
                on a.impactoption_idx=b.impactoption_idx
            inner join dim_rob_impact c
                on b.impact_idx=c.impact_idx
),
rob_impactoption_custsku as (
    select * from {{ ref('src_exc_fact_rob_impactoption_custsku')}}
),
rob_scenario as (
    select * from {{ ref('src_exc_fact_rob_scenario')}}
),
account_plan_actual as (
    select scen_idx,cust_idx,sku_idx,day_idx,ap_invoiced_sales_value,
    ap_gross_selling_value,tot_vol_sp_base_uom 
    from {{ ref('src_exc_fact_account_plan_actual')}}
),
account_plan as (
    select scen_idx,cust_idx,sku_idx,day_idx,ap_invoiced_sales_value,
    ap_gross_selling_value,tot_vol_sp_base_uom 
    from {{ ref('src_exc_fact_account_plan')}}
), 
fap as (
    select scen_idx,cust_idx,sku_idx,day_idx,ap_invoiced_sales_value,
    ap_gross_selling_value,tot_vol_sp_base_uom
    from account_plan_actual
    union all
    select scen_idx,cust_idx,sku_idx,day_idx,ap_invoiced_sales_value,
    ap_gross_selling_value,tot_vol_sp_base_uom 
    from account_plan
),
curr_conv_new_exc as (
    select * from {{ ref('stg_v_wtx_sls_curr_conv_new_exc')}}
),
source1 as(
    select 
        '{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system
        ,custskuday.cust_idx as cust_idx
,cust.code as plan_source_customer_code
,custskuday.sku_idx as sku_idx
,prod.code as source_item_identifier
,custskuday.day_idx as day_idx
,rob.rob_code as term_code   
,rob.rob_name as term_desc    
,rob.date_created as term_create_datetime  
,users.user_displayname as term_created_by   
,iff(impactoption_idx=1,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as rsa_perc
,iff(impactoption_idx=2,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as lump_sum
,iff(impactoption_idx=3,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as perc_invoiced_sales
,iff(impactoption_idx=4,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as perc_gross_sales
,iff(impactoption_idx=5,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as early_settlement_perc
,iff(impactoption_idx=6,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as edlp_perc
,iff(impactoption_idx=7,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as edlp_case_rate
, 0 as long_term_promo
,iff(impactoption_idx=8,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as rsi_perc
,iff(impactoption_idx=9,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as fixed_annual_payment
,iff(impactoption_idx=10,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as direct_shopper_marketing
,iff(impactoption_idx=11,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_direct_payment
,iff(impactoption_idx=12,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_direct_perc
,iff(impactoption_idx=13,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as category_payment
,iff(impactoption_idx=14,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as indirect_shopper_marketing
,iff(impactoption_idx=15,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_indirect_payment
,iff(impactoption_idx=16,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_indirect_perc
,iff(impactoption_idx=17,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as field_marketing
, 0 as consumer_spend
, dates.date_start as start_date
, dates.date_end as end_date
, status.status_code as status_code
, status.status_name as status_name
, status.verb as status_verb
, impact_option.impactoption_code as impact_option_code
, impact_option.impactoption_name as impact_option_name
, impact_option.impact_code as impact_code
, impact_option.impact_name as impact_name
, null as io_valvol_percent
, case when impact_option.islumpsumtype=true then '1' else '0' end as impact_option_lump_sum_flag
, impact_option.value as impact_option_value
, impact_option.financialimpactestimate as impact_option_fin_impact_estimate  
,iff(impactoption_idx=1,resultantvalue,0) as rsa_perc_trans
,iff(impactoption_idx=2,resultantvalue,0) as lump_sum_trans
,iff(impactoption_idx=3,resultantvalue,0) as perc_invoiced_sales_trans
,iff(impactoption_idx=4,resultantvalue,0) as perc_gross_sales_trans
,iff(impactoption_idx=5,resultantvalue,0) as early_settlement_perc_trans
,iff(impactoption_idx=6,resultantvalue,0) as edlp_perc_trans
,iff(impactoption_idx=7,resultantvalue,0) as edlp_case_rate_trans
,iff(impactoption_idx=8,resultantvalue,0) as rsi_perc_trans
,iff(impactoption_idx=9,resultantvalue,0) as fixed_annual_payment_trans
,iff(impactoption_idx=10,resultantvalue,0) as direct_shopper_marketing_trans
,iff(impactoption_idx=11,resultantvalue,0) as other_direct_payment_trans
,iff(impactoption_idx=12,resultantvalue,0) as other_direct_perc_trans
,iff(impactoption_idx=13,resultantvalue,0) as category_payment_trans
,iff(impactoption_idx=14,resultantvalue,0) as indirect_shopper_marketing_trans
,iff(impactoption_idx=15,resultantvalue,0) as other_indirect_payment_trans
,iff(impactoption_idx=16,resultantvalue,0) as other_indirect_perc_trans
,iff(impactoption_idx=17,resultantvalue,0) as field_marketing_trans
from rob_lumpsum_spread_custskuday custskuday
left outer join customer cust
on custskuday.cust_idx = cust.idx
left outer join product prod
on custskuday.sku_idx=prod.idx
left outer join rob rob  
on custskuday.rob_idx=rob.rob_idx 
left outer join users users
on users.user_idx = rob.rob_author_user_idx 
left outer join rob_dates dates
on custskuday.rob_idx=dates.rob_idx 
left outer join  rob_status status
on custskuday.rob_idx=status.rob_idx 
inner join rob_impactoption impact_option
    on custskuday.rob_idx=impact_option.rob_idx
    and custskuday.impactoption_idx=impact_option.io_idx
left outer join curr_conv_new_exc curr_x
  on cust.currency_idx = curr_x.from_curr_idx
  and 'GBP' = curr_x.to_curr
  and date(custskuday.day_idx,'YYYYMMDD') >= curr_x.eff_start_date
  and date(custskuday.day_idx,'YYYYMMDD') <= curr_x.eff_end_date
where impact_option.value<>0
and custskuday.scen_idx = 1
),
custskuday as (
select iocs.rob_idx
,fap.scen_idx
,fap.cust_idx
,fap.sku_idx
,fap.day_idx
,iocs.impactoption_idx
---14-mar-2021 - per the migration to new exceedra the calc for rsa % are to be driven off of gross sales instead of invoiced sales.  moving the io code of 'io_rsapcval' from one side to the other.
,case when iocs.impactoption_idx in (select impactoption_idx 
from dim_rob_impactoption where impactoption_code 
in ('IO_ORDpc', 'IO_ESDpcVal', 'IO_EDLPpcVal', 'IO_RSIpcVal', 'IO_ODPpcVal', 'IO_OIPpcVal')) 
		then iocs.value / 1.0 * fap.ap_invoiced_sales_value   --percent of invoice sales
		when iocs.impactoption_idx in (select impactoption_idx 
        from dim_rob_impactoption where impactoption_code 
        in ('IO_RSApcVal','IO_ORDpc_2')) 
		then iocs.value / 1.0 * fap.ap_gross_selling_value   --percent of gsv
		when iocs.impactoption_idx in (select impactoption_idx 
        from dim_rob_impactoption where impactoption_code 
        in ('IO_EDLPvVal', 'IO_LTPVal' )) 
		then iocs.value * fap.tot_vol_sp_base_uom   --value per case
		else 0 end as resultantvalue
,iocs.value as rob_value
from rob_impactoption_custsku iocs 
join rob_scenario rs
	on rs.rob_idx = iocs.rob_idx
inner join fap
	on fap.scen_idx = rs.scen_idx 
	and fap.cust_idx = iocs.cust_idx
	and fap.sku_idx  = iocs.sku_idx
	and fap.day_idx  between iocs.date_start_idx and iocs.date_end_idx
where iocs.impactoption_idx in (select drio.impactoption_idx 
from dim_rob_impactoption drio where drio.islumpsumtype = 0 and fap.scen_idx=1) 
),
source2 as(
select 
'{{env_var("DBT_SOURCE_SYSTEM")}}' as source_system
,custskuday.cust_idx as cust_idx
,trim(cust.code) as plan_source_customer_code
,custskuday.sku_idx as sku_idx
,trim(prod.code) as source_item_identifier
,custskuday.day_idx as day_idx
,rob.rob_code as term_code
,rob.rob_name as term_desc
,rob.date_created as term_create_datetime
,users.user_displayname as term_created_by
,iff(custskuday.impactoption_idx=1,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as rsa_perc
,iff(custskuday.impactoption_idx=2,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as lump_sum
,iff(custskuday.impactoption_idx=3,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as perc_invoiced_sales
,iff(custskuday.impactoption_idx=4,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as perc_gross_sales
,iff(custskuday.impactoption_idx=5,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as early_settlement_perc
,iff(custskuday.impactoption_idx=6,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as edlp_perc
,iff(custskuday.impactoption_idx=7,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as edlp_case_rate
, 0 as long_term_promo
,iff(custskuday.impactoption_idx=8,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as rsi_perc
,iff(custskuday.impactoption_idx=9,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as fixed_annual_payment
,iff(custskuday.impactoption_idx=10,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as direct_shopper_marketing
,iff(custskuday.impactoption_idx=11,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_direct_payment
,iff(custskuday.impactoption_idx=12,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_direct_perc
,iff(custskuday.impactoption_idx=13,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as category_payment
,iff(custskuday.impactoption_idx=14,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as indirect_shopper_marketing
,iff(custskuday.impactoption_idx=15,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_indirect_payment
,iff(custskuday.impactoption_idx=16,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as other_indirect_perc
,iff(custskuday.impactoption_idx=17,resultantvalue,0) * nvl(curr_x.conversion_rate,1) as field_marketing
, 0 as consumer_spend
, dates.date_start as start_date
, dates.date_end as end_date
, status.status_code as status_code
, status.status_name as status_name
, status.verb as status_verb
, impact_option.impactoption_code as impact_option_code
, impact_option.impactoption_name as impact_option_name
, impact_option.impact_code as impact_code
, impact_option.impact_name as impact_name
, null as io_valvol_percent
, case when impact_option.islumpsumtype=true then '1' else '0' end as impact_option_lump_sum_flag
, impact_option.value as impact_option_value
, impact_option.financialimpactestimate as impact_option_fin_impact_estimate
,iff(custskuday.impactoption_idx=1,resultantvalue,0) as rsa_perc_trans
,iff(custskuday.impactoption_idx=2,resultantvalue,0) as lump_sum_trans
,iff(custskuday.impactoption_idx=3,resultantvalue,0) as perc_invoiced_sales_trans
,iff(custskuday.impactoption_idx=4,resultantvalue,0) as perc_gross_sales_trans
,iff(custskuday.impactoption_idx=5,resultantvalue,0) as early_settlement_perc_trans
,iff(custskuday.impactoption_idx=6,resultantvalue,0) as edlp_perc_trans
,iff(custskuday.impactoption_idx=7,resultantvalue,0) as edlp_case_rate_trans
,iff(custskuday.impactoption_idx=8,resultantvalue,0) as rsi_perc_trans
,iff(custskuday.impactoption_idx=9,resultantvalue,0) as fixed_annual_payment_trans
,iff(custskuday.impactoption_idx=10,resultantvalue,0) as direct_shopper_marketing_trans
,iff(custskuday.impactoption_idx=11,resultantvalue,0) as other_direct_payment_trans
,iff(custskuday.impactoption_idx=12,resultantvalue,0) as other_direct_perc_trans
,iff(custskuday.impactoption_idx=13,resultantvalue,0) as category_payment_trans
,iff(custskuday.impactoption_idx=14,resultantvalue,0) as indirect_shopper_marketing_trans
,iff(custskuday.impactoption_idx=15,resultantvalue,0) as other_indirect_payment_trans
,iff(custskuday.impactoption_idx=16,resultantvalue,0) as other_indirect_perc_trans
,iff(custskuday.impactoption_idx=17,resultantvalue,0) as field_marketing_trans
from custskuday
    left outer join rob rob
				on rob.rob_idx = custskuday.rob_idx
	left outer join rob_dates rd
				on rd.rob_idx = custskuday.rob_idx
	left outer join customer cust
				on custskuday.cust_idx = cust.idx
    left outer join product prod
                on custskuday.sku_idx=prod.idx
    left outer join users users
                on users.user_idx = rob.rob_author_user_idx
    left outer join rob_dates dates
                on custskuday.rob_idx=dates.rob_idx 
    inner join rob_impactoption1 impact_option
           on custskuday.rob_idx=impact_option.rob_idx  
		   and custskuday.impactoption_idx=impact_option.impactoption_idx  
    inner join rob_status status
			on custskuday.rob_idx=status.rob_idx
     left outer join curr_conv_new_exc curr_x
      on cust.currency_idx = curr_x.from_curr_idx
      anD 'GBP' = curr_x.to_curr
      and date(custskuday.day_idx,'YYYYMMDD') >= curr_x.eff_start_date
      and date(custskuday.day_idx,'yyyymmdd') <= curr_x.eff_end_date       
where custskuday.scen_idx = 1 and custskuday.resultantvalue <> 0
and impact_option.value<>0 
),
source_union as (
    select * from source1
    union
    select * from source2
),
final as (
select 
  source_system                       as source_system
        , cust_idx                          as cust_idx            
        , plan_source_customer_code 
        , sku_idx
        , source_item_identifier
        , day_idx
        , term_code   
        , term_desc    
        , term_create_datetime  
        , term_created_by   
        , sum(rsa_perc)                     as rsa_perc
        , sum(lump_sum)                     as lump_sum
        , sum(perc_invoiced_sales)          as perc_invoiced_sales
        , sum(perc_gross_sales)             as perc_gross_sales
        , sum(early_settlement_perc)        as early_settlement_perc
        , sum(edlp_perc)                    as edlp_perc
        , sum(edlp_case_rate)               as edlp_case_rate
        , sum(long_term_promo)              as long_term_promo
        , sum(rsi_perc)                     as rsi_perc
        , sum(fixed_annual_payment)         as fixed_annual_payment
        , sum(direct_shopper_marketing)     as direct_shopper_marketing
        , sum(other_direct_payment)         as other_direct_payment
        , sum(other_direct_perc)            as other_direct_perc
        , sum(category_payment)             as category_payment
        , sum(indirect_shopper_marketing)   as indirect_shopper_marketing
        , sum(other_indirect_payment)       as other_indirect_payment
        , sum(other_indirect_perc)          as other_indirect_perc
        , sum(field_marketing)              as field_marketing
        , sum(consumer_spend)               as consumer_spend
        , start_date                        as term_start_date
        , end_date                          as term_end_date
        , status_code
        , status_name
        , status_verb
        , impact_option_code
        , impact_option_name
        , impact_code
        , impact_name
        , sum(io_valvol_percent)                    as impact_option_valvol_percent
        , impact_option_lump_sum_flag
        , max(impact_option_value)                  as impact_option_value
        , max(impact_option_fin_impact_estimate)    as impact_option_fin_impact_estimate
        , sum(rsa_perc_trans)                       as rsa_perc_trans
        , sum(lump_sum_trans)                       as lump_sum_trans
        , sum(perc_invoiced_sales_trans)            as perc_invoiced_sales_trans
        , sum(perc_gross_sales_trans)               as perc_gross_sales_trans
        , sum(early_settlement_perc_trans)          as early_settlement_perc_trans
        , sum(edlp_perc_trans)                      as edlp_perc_trans
        , sum(edlp_case_rate_trans)                 as edlp_case_rate_trans
        , sum(rsi_perc_trans)                       as rsi_perc_trans
        , sum(fixed_annual_payment_trans)           as fixed_annual_payment_trans
        , sum(direct_shopper_marketing_trans)       as direct_shopper_marketing_trans
        , sum(other_direct_payment_trans)           as other_direct_payment_trans
        , sum(other_direct_perc_trans)              as other_direct_perc_trans
        , sum(category_payment_trans)               as category_payment_trans
        , sum(indirect_shopper_marketing_trans)     as indirect_shopper_marketing_trans
        , sum(other_indirect_payment_trans)         as other_indirect_payment_trans
        , sum(other_indirect_perc_trans)            as other_indirect_perc_trans
        , sum(field_marketing_trans)                as field_marketing_trans

from source_union
group by 
source_system
, cust_idx
, plan_source_customer_code
, sku_idx
, source_item_identifier
, day_idx
, term_code   
, term_desc    
, term_create_datetime  
, term_created_by   
, start_date
, end_date
, status_code
, status_name
, status_verb
, impact_option_code
, impact_option_name
, impact_code
, impact_name
, impact_option_lump_sum_flag
)
select
source_system,
	cust_idx,
	plan_source_customer_code,
	sku_idx,
	source_item_identifier,
	day_idx,
	term_code,
	term_desc,
	term_create_datetime,
	term_created_by,
	rsa_perc,
	lump_sum,
	perc_invoiced_sales,
	perc_gross_sales,
	early_settlement_perc,
	edlp_perc,
	edlp_case_rate,
	long_term_promo,
	rsi_perc,
	fixed_annual_payment,
	direct_shopper_marketing,
	other_direct_payment,
	other_direct_perc,
	category_payment,
	indirect_shopper_marketing,
	other_indirect_payment,
	other_indirect_perc,
	field_marketing,
	consumer_spend,
	term_start_date,
	term_end_date,
	status_code,
	status_name,
	status_verb,
	impact_option_code,
	impact_option_name,
	impact_code,
	impact_name,
	impact_option_valvol_percent,
	impact_option_lump_sum_flag,
	impact_option_value,
	impact_option_fin_impact_estimate,
	rsa_perc_trans,
	lump_sum_trans,
	perc_invoiced_sales_trans,
	perc_gross_sales_trans,
	early_settlement_perc_trans,
	edlp_perc_trans,
	edlp_case_rate_trans,
	rsi_perc_trans,
	fixed_annual_payment_trans,
	direct_shopper_marketing_trans,
	other_direct_payment_trans,
	other_direct_perc_trans,
	category_payment_trans,
	indirect_shopper_marketing_trans,
	other_indirect_payment_trans,
	other_indirect_perc_trans,
	field_marketing_trans 
    from final