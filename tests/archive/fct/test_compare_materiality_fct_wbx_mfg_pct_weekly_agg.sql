{{ config( 
    enabled=false,
    severity = 'warn',
    warn_if = '>1'  
) }} 


select value_type,"'IICS'" IICS,"'DBT'" DBT, "'IICS'"-"'DBT'" difference, abs(div0null(("'IICS'"-"'DBT'"),"'IICS'")*100) "difference_perc%" from 
(
select * from (
select * from (
select 
'DBT' as source_system,
sum(trunc( demand_transit_qty , 2 ) ) as demand_transit_qty ,
sum(trunc( supply_transit_qty , 2 ) ) as supply_transit_qty ,
sum(trunc( expired_qty , 2 ) ) as expired_qty ,
sum(trunc( blocked_qty , 2 ) ) as blocked_qty ,
sum(trunc( pa_eff_week_qty , 2 ) ) as pa_eff_week_qty ,
sum(trunc( pa_balance_qty , 2 ) ) as pa_balance_qty ,
sum(trunc( virtual_stock_qty , 2 ) ) as virtual_stock_qty ,
sum(trunc( demand_wo_qty , 2 ) ) as demand_wo_qty ,
sum(trunc( production_wo_qty , 2 ) ) as production_wo_qty ,
sum(trunc( production_planned_wo_qty , 2 ) ) as production_planned_wo_qty ,
sum(trunc( demand_planned_batch_wo_qty , 2 ) ) as demand_planned_batch_wo_qty ,
sum(trunc( demand_unplanned_batch_wo_qty , 2 ) ) as demand_unplanned_batch_wo_qty ,
sum(trunc( supply_trans_journal_qty , 2 ) ) as supply_trans_journal_qty ,
sum(trunc( demand_trans_journal_qty , 2 ) ) as demand_trans_journal_qty ,
sum(trunc( demand_planned_trans_qty , 2 ) ) as demand_planned_trans_qty ,
sum(trunc( supply_stock_journal_qty , 2 ) ) as supply_stock_journal_qty ,
sum(trunc( demand_stock_journal_qty , 2 ) ) as demand_stock_journal_qty ,
sum(trunc( supply_po_qty , 2 ) ) as supply_po_qty ,
sum(trunc( supply_planned_po_qty , 2 ) ) as supply_planned_po_qty ,
sum(trunc( supply_po_transfer_qty , 2 ) ) as supply_po_transfer_qty ,
sum(trunc( supply_po_return_qty , 2 ) ) as supply_po_return_qty ,
sum(trunc( sales_order_qty , 2 ) ) as sales_order_qty ,
sum(trunc( return_sales_order_qty , 2 ) ) as return_sales_order_qty ,
sum(trunc( minimum_stock_qty , 2 ) ) as minimum_stock_qty ,
sum(trunc( week_end_stock , 2 ) ) as week_end_stock ,
sum(trunc( period_end_firm_purchase_qty , 2 ) ) as period_end_firm_purchase_qty ,
sum(trunc( supply_stock_adj_qty , 2 ) ) as supply_stock_adj_qty ,
sum(trunc( demand_stock_adj_qty , 2 ) ) as demand_stock_adj_qty 


from  {{ref('fct_wbx_mfg_pct_weekly_agg')}}

union

select 
'IICS' as source_system,
sum(trunc( demand_transit_qty , 2 ) ) as demand_transit_qty ,
sum(trunc( supply_transit_qty , 2 ) ) as supply_transit_qty ,
sum(trunc( expired_qty , 2 ) ) as expired_qty ,
sum(trunc( blocked_qty , 2 ) ) as blocked_qty ,
sum(trunc( pa_eff_week_qty , 2 ) ) as pa_eff_week_qty ,
sum(trunc( pa_balance_qty , 2 ) ) as pa_balance_qty ,
sum(trunc( virtual_stock_qty , 2 ) ) as virtual_stock_qty ,
sum(trunc( demand_wo_qty , 2 ) ) as demand_wo_qty ,
sum(trunc( production_wo_qty , 2 ) ) as production_wo_qty ,
sum(trunc( production_planned_wo_qty , 2 ) ) as production_planned_wo_qty ,
sum(trunc( demand_planned_batch_wo_qty , 2 ) ) as demand_planned_batch_wo_qty ,
sum(trunc( demand_unplanned_batch_wo_qty , 2 ) ) as demand_unplanned_batch_wo_qty ,
sum(trunc( supply_trans_journal_qty , 2 ) ) as supply_trans_journal_qty ,
sum(trunc( demand_trans_journal_qty , 2 ) ) as demand_trans_journal_qty ,
sum(trunc( demand_planned_trans_qty , 2 ) ) as demand_planned_trans_qty ,
sum(trunc( supply_stock_journal_qty , 2 ) ) as supply_stock_journal_qty ,
sum(trunc( demand_stock_journal_qty , 2 ) ) as demand_stock_journal_qty ,
sum(trunc( supply_po_qty , 2 ) ) as supply_po_qty ,
sum(trunc( supply_planned_po_qty , 2 ) ) as supply_planned_po_qty ,
sum(trunc( supply_po_transfer_qty , 2 ) ) as supply_po_transfer_qty ,
sum(trunc( supply_po_return_qty , 2 ) ) as supply_po_return_qty ,
sum(trunc( sales_order_qty , 2 ) ) as sales_order_qty ,
sum(trunc( return_sales_order_qty , 2 ) ) as return_sales_order_qty ,
sum(trunc( minimum_stock_qty , 2 ) ) as minimum_stock_qty ,
sum(trunc( week_end_stock , 2 ) ) as week_end_stock ,
sum(trunc( period_end_firm_purchase_qty , 2 ) ) as period_end_firm_purchase_qty ,
sum(trunc( supply_stock_adj_qty , 2 ) ) as supply_stock_adj_qty ,
sum(trunc( demand_stock_adj_qty , 2 ) ) as demand_stock_adj_qty 

from  {{ref('conv_inv_wtx_pct_wkly_agg')}}

)
unpivot(value_of for value_type in 
          (
demand_transit_qty ,
supply_transit_qty ,
expired_qty ,
blocked_qty ,
pa_eff_week_qty ,
pa_balance_qty ,
virtual_stock_qty ,
demand_wo_qty ,
production_wo_qty ,
production_planned_wo_qty ,
demand_planned_batch_wo_qty ,
demand_unplanned_batch_wo_qty ,
supply_trans_journal_qty ,
demand_trans_journal_qty ,
demand_planned_trans_qty ,
supply_stock_journal_qty ,
demand_stock_journal_qty ,
supply_po_qty ,
supply_planned_po_qty ,
supply_po_transfer_qty ,
supply_po_return_qty ,
sales_order_qty ,
return_sales_order_qty ,
minimum_stock_qty ,
week_end_stock ,
period_end_firm_purchase_qty ,
supply_stock_adj_qty ,
demand_stock_adj_qty 

)
)   
)
 pivot(sum(value_of) for source_system in ('IICS', 'DBT')) 
      as p
)		  where difference>0
