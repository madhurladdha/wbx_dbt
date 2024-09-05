/*
set (months_of_hist,key_mix_hist,single_day_bool,exclude_covid) = (18,6,0,0)
This view requires the above Snowflake environment variables to be set before the view and be queried.
months_of_hist: The number of historical months pulled into the view
key_mix_hist: How many months a key is included in the data after it's most recent invoice
single_day_bool: Flag. If 1, only the most recent snapshot date (VDATE) is returned.
exclude_covid: Flag. If 1, Mar-May 2020 are exluded.
Note that months_of_hist and key_mix_hist are still impactful even when single_day_bool is 1 as we need to know what keys existed x months ago
CTE_ORDERS queries data at the order (line) grain.
Plug in the most relevant fields from the order table that fit the descriptions of the fields
*/

{{ config(
    schema='CLOUD_ML',
  sql_header="set (months_of_hist,key_mix_hist,single_day_bool,exclude_covid) = (18,6,0,0);"
) }}

with days as(
    select * from {{ref('src_proj_daysinmonth_xref')}}
),

ord as(
    select * from {{ref('v_wtx_sales_summary')}}
),

date as(
    select * from {{ref('src_dim_date')}}
),

cte_orders as (
select      
to_varchar(ord.sub_product||'-'||ord.pack_size_seq) as ml_item_dim,  -- forecast key, deepest item grain
to_varchar(trade_type_seq) as ml_cust_dim, -- forecast key, deepest customer grain
'NOT AT THE DC LEVEL'      as ml_dc_dim,           -- forecast key, deepest facility grain
trunc(ord.trans_confirmdate_confirmed, 'dd')   as ml_order_dt,         -- Preferably unchanged over life of order.
trunc(ord.line_actual_ship_date, 'dd') as ml_invoice_dt,       
trunc(ord.line_cancelled_date, 'dd')    as ml_cancel_dt,       
trunc(coalesce(ord.line_original_promised_date, ord.line_requested_date, ord.line_prom_ship_date), 'dd')     as ml_expect_dt,        -- preferably unchanged over life of order. whether this date is "promised", "requested", "expected" or similar differs by business
trunc(coalesce(
    ord.line_actual_ship_date,  
    ord.line_invoice_date,
    ord.line_cancelled_date,
    trunc(current_date, 'dd') - 1 --this needs a timezone conversion for prod
    ), 'dd')   as ml_synthetic_end_dt, -- this is a combination of the above dates, driven by order status (invoiced, open, canceled, etc.)
ord.base_lineamount_confirmed   as ml_order,        -- preferably unchanged over life of order. either dollars or volume, whichever the business prefers to measure accuracy by
ord.base_invoice_grs_amt        as ml_invoice,    
ord.short_kg_quantity * ord.base_rpt_grs_kg_price   as ml_short,    
ord.cancel_kg_quantity * ord.base_rpt_grs_kg_price  as ml_cancel,  
ord.base_rpt_grs_prim_amt    as ml_pending,      -- this captures qty that is currently pending. if the business has a true ordered qty, use that. otherwise use live pending.
ord.base_invoice_grs_amt  as ml_target,
ord.lead_time,
ord.sales_order_number,
ord.base_rpt_grs_kg_price
from  ord
WHERE 
ORD.SALES_ORDER_COMPANY = 'WBX' 
and ord.trans_confirmdate_confirmed > dateadd('month', -($months_of_hist)-3, date_trunc('month',current_date))
and ord.trade_type_seq is not null
),
/*CTE_SNAPSHOTS turns each single order record into a set of records for every day between the order date and the synthetic end date */


CTE_SNAPSHOTS as
 (
select 
calendar_date vdate, --functions as the snapshot date
date_trunc('month', to_date(calendar_date)) vdate_month,
cte_orders.*,
case
when ml_invoice_dt is null then ml_pending
when ml_invoice_dt = vdate then 0
else ml_order
end ml_snap_pending,
case
  when ml_invoice_dt is null
  and date_trunc('month',ml_expect_dt) <= date_trunc('month',vdate) then ml_pending
  when vdate >= ml_cancel_dt and ml_cancel_dt is not null then 0
  when ml_invoice_dt = vdate then 0
  when date_trunc('month',ml_expect_dt) <= date_trunc('month',vdate) then ml_order
  else 0
  end as ml_curr_snap_pending,
    IFF(ML_INVOICE_DT = VDATE,ML_TARGET,0) ML_SNAP_TARGET,
    IFF(datediff('day',ML_EXPECT_DT,ML_INVOICE_DT)>=2,ML_SNAP_TARGET,0) ML_DELAYED_TARGET,
    iff(ml_invoice_dt = vdate,ml_short,0) ml_snap_short,
    iff(ml_invoice_dt = vdate,ml_cancel,0) ml_snap_cancel,
    iff(ml_order_dt = vdate, sales_order_number, null) as order_number,
    datediff(day,vdate,ml_expect_dt) as exp_days_to_close
    from  date
    left join cte_orders
    on date.calendar_date between cte_orders.ml_order_dt and ml_synthetic_end_dt where ml_order_dt is not null
    and calendar_date between trunc(dateadd(month, -$months_of_hist,to_date(current_date)),'mm') and trunc(current_date,'dd')-1
),

/* CTE_KEY_AGG aggregates the grain to the key: the snapshot date (VDATE) plus dimensions for item, customer and DC */

cte_key_agg as (
    select
        ml_cust_dim,
        ml_item_dim,
        ml_dc_dim,
        vdate vdate,
        avg(base_rpt_grs_kg_price) avg_kg_price, 
        sum(ml_curr_snap_pending) ml_curr_snap_pending,
        sum(ml_snap_target) ml_snap_target,
        sum(ml_delayed_target) ml_delayed_target,
        sum(ml_snap_short) ml_snap_short,
        sum(ml_snap_cancel) ml_snap_cancel,
        avg(exp_days_to_close) exp_days_to_close,
        avg(lead_time) lead_time,
        max(ml_order_dt) most_recent_order,
        count(distinct cte_snapshots.order_number) num_orders,
        sum(
            nvl(
                ml_snap_pending / greatest(1,datediff('dd',vdate,ml_expect_dt)),0 )) as weighted_days_to_close

    from
    cte_snapshots
    group by
        vdate,
        ml_cust_dim,
        ml_item_dim,
        ml_dc_dim
),
/*CTE_KEY_MIX creates a single record for every month that a cust/item/DC has an open order, as well as N months after that month (where N is session variable $key_mix_hist) */
CTE_KEY_MIX AS (
select 
calendar_month_start_dt,
ml_cust_dim,
ml_item_dim,
ml_dc_dim,       
min(ml_order_dt) min_ml_order_dt,
iff(min(ml_order_dt) > calendar_month_start_dt,1,0) first_month_flag
from {{ref('src_dim_date')}} dd
left join cte_snapshots
on calendar_month_start_dt between date_trunc('month', vdate) and dateadd('mm', $key_mix_hist, date_trunc('month', vdate))
where dd.calendar_month_start_dt = dd.calendar_date
and calendar_month_start_dt between dateadd('mm', -$months_of_hist, trunc(current_date, 'mm')) and trunc(current_date, 'mm')
group by calendar_month_start_dt, ml_cust_dim, ml_item_dim, ml_dc_dim
),

-- CTE_CARTESIAN takes CTE_KEY_MIX (at a monthly grain) and cartesians it such that there is a record for every day in those months. Then left joins in the data from CTE_KEY_AGG.
-- The effect is that that each key now has records on days that they didn't have any open orders

cte_cartesian as (
    select
        dd.calendar_date vdate,
        cte_key_mix.ml_cust_dim,
        cte_key_mix.ml_item_dim,
        cte_key_mix.ml_dc_dim,
        dd.calendar_month_start_dt,
        nvl(cte_key_agg.avg_kg_price, 0) avg_kg_price,
        nvl(cte_key_agg.ml_curr_snap_pending, 0) ml_curr_snap_pending,
        nvl(cte_key_agg.ml_snap_target, 0) ml_snap_target,
        nvl(cte_key_agg.ml_delayed_target, 0) ml_delayed_target,
        nvl(cte_key_agg.exp_days_to_close, 0) exp_days_to_close,
        nvl(cte_key_agg.weighted_days_to_close, 0) weighted_days_to_close,
        nvl(cte_key_agg.ml_snap_short, 0) ml_snap_short,
        nvl(cte_key_agg.ml_snap_cancel, 0) ml_snap_cancel,
        nvl(cte_key_agg.lead_time, 0) lead_time,
        nvl(cte_key_agg.num_orders, 0) num_orders,
        nvl(cte_key_agg.most_recent_order, last_value(cte_key_agg.most_recent_order) ignore nulls over (partition by cte_key_mix.ml_cust_dim, cte_key_mix.ml_item_dim, cte_key_mix.ml_dc_dim order by vdate asc)) as most_recent_order,
        nvl(datediff(day, most_recent_order, vdate), 0) as days_since_last_order
    from date DD
        left join cte_key_mix on cte_key_mix.calendar_month_start_dt = dd.calendar_month_start_dt full
        outer join cte_key_agg on date_trunc('month',cte_key_agg.vdate) = dd.calendar_month_start_dt
        and cte_key_agg.vdate = dd.calendar_date
        and cte_key_agg.ml_cust_dim = cte_key_mix.ml_cust_dim
        and cte_key_agg.ml_item_dim = cte_key_mix.ml_item_dim
        and cte_key_agg.ml_dc_dim = cte_key_mix.ml_dc_dim
    WHERE
        dd.calendar_date between dateadd('mm', -$months_of_hist, trunc(current_date, 'mm'))
        and date(dateadd('day', -1, date_trunc('day', current_date)))
        and (cte_key_mix.first_month_flag = 0 or cte_key_mix.min_ml_order_dt <= dd.calendar_date)
),
       
-- CTE_DAILY calculates sales by weekday from the previous month for each key

cte_daily as (
    select * from
    (
        select
            dateadd('month', 1, cte_cartesian.calendar_month_start_dt) calendar_month_start_dt,
            cte_cartesian.ml_cust_dim,
            cte_cartesian.ml_item_dim,
            cte_cartesian.ml_dc_dim,
            dayofweek(cte_cartesian.vdate) dow,
            avg(cte_cartesian.ml_snap_target) ml_snap_target
        from
            cte_cartesian
        group by
            cte_cartesian.ml_cust_dim,
            cte_cartesian.ml_item_dim,
            cte_cartesian.ml_dc_dim,
            dayofweek(cte_cartesian.vdate),
            dateadd('month', 1, cte_cartesian.calendar_month_start_dt)
        order by
            dateadd('month', 1, cte_cartesian.calendar_month_start_dt),
            dayofweek(cte_cartesian.vdate),
            cte_cartesian.ml_cust_dim,
            cte_cartesian.ml_item_dim
    ) pivot(
        sum(ml_snap_target) for dow in (0, 1, 2, 3, 4, 5, 6)
    ) as p (
        calendar_month_start_dt,
        ml_cust_dim,
        ml_item_dim,
        ml_dc_dim,
        sunday,
        monday,
        tuesday,
        wednesday,
        thursday,
        friday,
        saturday
    )
),


prefinal as(
select * from (
select
    cte_cartesian.vdate,
    cte_cartesian.ml_cust_dim,
    cte_cartesian.ml_item_dim,
    cte_cartesian.ml_dc_dim,
    cte_cartesian.avg_kg_price,
    cte_cartesian.calendar_month_start_dt year_month,
    cte_cartesian.ml_curr_snap_pending,
    cte_cartesian.ml_snap_target,
    sum(nvl(cte_cartesian.ml_snap_target, 0)) over (
        partition by cte_cartesian.ml_cust_dim,
        cte_cartesian.ml_item_dim,
        cte_cartesian.ml_dc_dim,
        cte_cartesian.calendar_month_start_dt
        order by
            cte_cartesian.vdate
    ) cum_invoiced_tgt,
    sum(nvl(cte_cartesian.ml_snap_target, 0)) over (
        partition by cte_cartesian.ml_cust_dim,
        cte_cartesian.ml_item_dim,
        cte_cartesian.ml_dc_dim,
        cte_cartesian.calendar_month_start_dt
    ) eom_invoiced_tgt,

    eom_invoiced_tgt - cum_invoiced_tgt yet_to_be_invoiced,
    cte_cartesian.exp_days_to_close,
    cte_cartesian.weighted_days_to_close,
    cte_cartesian.days_since_last_order,
    nvl(daily.monday * days.mondays_left, 0) tgt_per_monday,
    nvl(daily.tuesday * days.tuesdays_left, 0) tgt_per_tuesday,
  nvl(daily.wednesday * days.wednesdays_left, 0) tgt_per_wednesday,
    nvl(daily.thursday * days.thursdays_left, 0) tgt_per_thursday,
    nvl(daily.friday * days.fridays_left, 0) tgt_per_friday,
    nvl(daily.saturday * days.saturdays_left, 0) tgt_per_saturday,
    nvl(daily.sunday * days.sundays_left, 0) tgt_per_sunday,
    nvl(daily.sunday * days.holidays_left, 0) tgt_per_holiday,
    nvl(avg(cte_cartesian.ml_snap_target) over (partition by cte_cartesian.ml_cust_dim,cte_cartesian.ml_item_dim order by cte_cartesian.vdate rows between 27 preceding and 0 preceding),0) target_movavg_28,
    nvl(avg(cte_cartesian.ml_snap_target) over (partition by cte_cartesian.ml_cust_dim,cte_cartesian.ml_item_dim order by cte_cartesian.vdate rows between 41 preceding and 0 preceding),0) target_movavg_42,
    nvl(avg(cte_cartesian.ml_delayed_target) over (partition by cte_cartesian.ml_cust_dim,cte_cartesian.ml_item_dim order by cte_cartesian.vdate rows between 55 preceding and 0 preceding),0) delayed_movavg_56,
    nvl(mondays_left+tuesdays_left+wednesdays_left+thursdays_left+fridays_left+saturdays_left+sundays_left+holidays_left,0) days_left,
    case when days_left = 0 then 0 else yet_to_be_invoiced / days_left end as ytbi_per_day,
    case when days_left = 0 then 0 else ml_curr_snap_pending / days_left end as pending_per_day,
    days_left - saturdays_left-sundays_left-holidays_left weekdays_left,
    tgt_per_monday+tgt_per_tuesday+tgt_per_wednesday+tgt_per_thursday+tgt_per_friday+tgt_per_holiday as naive,
    nvl(sum(cte_cartesian.num_orders) over (partition by cte_cartesian.ml_cust_dim, cte_cartesian.ml_item_dim, cte_cartesian.ml_dc_dim order by cte_cartesian.vdate 
                                       rows between 61 preceding and 1 preceding)/60,0) avg_time_btwn_orders_last_60,
    nvl(avg(cte_cartesian.lead_time) over (partition by cte_cartesian.ml_cust_dim, cte_cartesian.ml_item_dim, cte_cartesian.ml_dc_dim order by cte_cartesian.vdate 
                                       rows between 61 preceding and 1 preceding),0) avg_lead_time_60

                  from cte_cartesian
    left join cte_daily daily on daily.ml_cust_dim = cte_cartesian.ml_cust_dim
    and daily.ml_item_dim = cte_cartesian.ml_item_dim
    and daily.ml_dc_dim = cte_cartesian.ml_dc_dim
    and daily.calendar_month_start_dt = cte_cartesian.calendar_month_start_dt
    left join  days on to_date(days.date_id, 'yyyymmdd') = cte_cartesian.vdate
    )
where iff($single_day_bool = 1,vdate,'9999-01-01') = iff($single_day_bool = 1,current_date-1,'9999-01-01')
and iff($exclude_covid=1,year_month,'9999-01-01') not in ('2020-03-01','2020-04-01','2020-05-01')
)



select 
	VDATE,
	ML_CUST_DIM,
	ML_ITEM_DIM,
	ML_DC_DIM,
	AVG_KG_PRICE,
	YEAR_MONTH,
	ML_CURR_SNAP_PENDING,
	ML_SNAP_TARGET,
	CUM_INVOICED_TGT,
	EOM_INVOICED_TGT,
	YET_TO_BE_INVOICED,
	EXP_DAYS_TO_CLOSE,
	WEIGHTED_DAYS_TO_CLOSE,
	DAYS_SINCE_LAST_ORDER,
	TGT_PER_MONDAY,
	TGT_PER_TUESDAY,
	TGT_PER_WEDNESDAY,
	TGT_PER_THURSDAY,
	TGT_PER_FRIDAY,
	TGT_PER_SATURDAY,
	TGT_PER_SUNDAY,
	TGT_PER_HOLIDAY,
	TARGET_MOVAVG_28,
	TARGET_MOVAVG_42,
	DELAYED_MOVAVG_56,
	DAYS_LEFT,
	YTBI_PER_DAY,
	PENDING_PER_DAY,
	WEEKDAYS_LEFT,
	NAIVE,
	AVG_TIME_BTWN_ORDERS_LAST_60,
	AVG_LEAD_TIME_60
    from prefinal