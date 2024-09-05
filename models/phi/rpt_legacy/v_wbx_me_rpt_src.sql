{{ config(
    schema='CLOUD_ML'
  
) }}



with ml as(
    select * from {{ref('src_ml_wbx_month_end_forecast_fct')}}
),

sales as(
    select * from {{ref('v_wtx_sales_summary')}}
),

ord as(
select     
trade_sector_desc, 
market, 
sub_market, 
trade_class, 
trade_group, 
trade_type, 
manufacturing_group_desc, 
pack_size_desc, 
branding, 
product_class, 
sub_product,
pack_size_seq,
trade_type_seq 
from sales
group by 
trade_sector_desc, 
market, 
sub_market, 
trade_class, 
trade_group, 
trade_type, 
manufacturing_group_desc, 
pack_size_desc, 
branding, 
product_class, 
sub_product,
pack_size_seq,
trade_type_seq
),

final as(

select 
    ml.ML_CUST_DIM,
	ml.ML_ITEM_DIM,
	ml.ML_DC_DIM,
    ml.VDATE,
	ml.YEAR_MONTH,
	ml.CUM_INVOICED_TGT,
	ml.AVG_KG_PRICE,
	ml.YTBI_PER_DAY,
    ml.ML_CURR_SNAP_PENDING,
    ml.DAYS_LEFT,
    ml.WEEKDAYS_LEFT,
    ml.AVG_TIME_BTWN_ORDERS_LAST_60,
    ml.AVG_LEAD_TIME_60,
    ml.DAYS_SINCE_LAST_ORDER,
    ml.TARGET_MOVAVG_28,
	ml.EXP_DAYS_TO_CLOSE,
	ml.WEIGHTED_DAYS_TO_CLOSE,
	ml.DELAYED_MOVAVG_56,
    ml.TARGET_MOVAVG_42,
    ml.NAIVE,
    ml.PENDING_PER_DAY,
	ml.PREDICTION,
    ml.ML_SNAP_TARGET,
    ml.YTBI_PREDICTION,
    ml.YTBI_DAY_AVG_PREDICTION,
    ml.TGT_PER_HOLIDAY,
	ml.UPDATE_DATE,
	ml.LOAD_DATE,
    ord.trade_sector_desc, 
    ord.market, 
    ord.sub_market, 
    ord.trade_class, 
    ord.trade_group, 
    ord.trade_type, 
    ord.manufacturing_group_desc, 
    ord.pack_size_desc, 
    ord.branding, 
    ord.product_class, 
    ord.sub_product
from ml
left join 
ord
on ml.ml_item_dim = TO_VARCHAR(ORD.SUB_PRODUCT||'-'||ORD.PACK_SIZE_SEQ)
and ml.ml_cust_dim = TO_VARCHAR(ord.TRADE_TYPE_SEQ)
)


select * from final
