--adding a manual test instead to account for rounding offs and to check only for latest snapshot
--note that the cost fields have been commented out due to randomness even in the iics world
{{ config(
  enabled=false,
  severity = 'warn'
) }}

{% set old_etl_relation=ref('conv_inv_wtx_pct_wkly_agg') %}

{% set dbt_relation=ref('fct_wbx_mfg_pct_weekly_agg') %}


with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SNAPSHOT_DATE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "VARIANT_CODE"           ,      
    "SOURCE_COMPANY_CODE"           ,      
    "PLAN_VERSION"           ,      
    "WEEK_DESCRIPTION"           ,      
    "WEEK_START_DT"           ,      
    "WEEK_END_DT"           ,      
     round("WEEK_START_STOCK"           , 2),   
     round("DEMAND_TRANSIT_QTY"           ,       2), 
     round("SUPPLY_TRANSIT_QTY"           ,     2),   
     round("EXPIRED_QTY"           ,       2), 
     round("BLOCKED_QTY"           ,       2), 
     round("PA_EFF_WEEK_QTY"           ,       2), 
     round("PA_BALANCE_QTY"           ,       2), 
     round("VIRTUAL_STOCK_QTY"           ,       2), 
     round("DEMAND_WO_QTY"           ,       2), 
     round("PRODUCTION_WO_QTY"           ,       2), 
     round("PRODUCTION_PLANNED_WO_QTY"           ,       2), 
     round("DEMAND_PLANNED_BATCH_WO_QTY"           ,       2), 
     round("DEMAND_UNPLANNED_BATCH_WO_QTY"           ,       2), 
     round("SUPPLY_TRANS_JOURNAL_QTY"           ,       2), 
     round("DEMAND_TRANS_JOURNAL_QTY"           ,       2), 
     round("DEMAND_PLANNED_TRANS_QTY"           ,       2), 
     round("SUPPLY_STOCK_JOURNAL_QTY"           ,      2),  
     round("DEMAND_STOCK_JOURNAL_QTY"           , 2),       
     round("SUPPLY_PO_QTY"           ,       2), 
     round("SUPPLY_PLANNED_PO_QTY"           ,       2), 
     round("SUPPLY_PO_TRANSFER_QTY"           ,       2), 
     round("SUPPLY_PO_RETURN_QTY"           ,       2), 
     round("SALES_ORDER_QTY"           ,       2), 
     round("RETURN_SALES_ORDER_QTY"           ,       2), 
     round("MINIMUM_STOCK_QTY"           ,       2), 
     round("WEEK_END_STOCK"           ,       2), 
     "CURRENT_WEEK_FLAG"           ,       
     round("PERIOD_END_FIRM_PURCHASE_QTY"           ,  2),      
    "BASE_CURRENCY"           ,      
    "PHI_CURRENCY"           ,      
    "PCOMP_CURRENCY"           ,      
    --"OC_BASE_ITEM_UNIT_PRIM_COST"   --commenting these as there are dups in the item cost if lookup done only on item and variant code,dbt code adjusted to always pick the latest row,its random in iics        ,      
    --"OC_CORP_ITEM_UNIT_PRIM_COST"           ,      
    --"OC_PCOMP_ITEM_UNIT_PRIM_COST"           ,      
    "AGREEMENT_FLAG"           ,      
    "SUPPLY_STOCK_ADJ_QTY"           ,      
    "DEMAND_STOCK_ADJ_QTY"      
  

from {{ref('conv_inv_wtx_pct_wkly_agg')}} where snapshot_date=current_Date()


),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SNAPSHOT_DATE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "VARIANT_CODE"           ,      
    "SOURCE_COMPANY_CODE"           ,      
    "PLAN_VERSION"           ,      
    "WEEK_DESCRIPTION"           ,      
    "WEEK_START_DT"           ,      
    "WEEK_END_DT"           ,      
       round("WEEK_START_STOCK"           , 2),       
     round("DEMAND_TRANSIT_QTY"           ,       2), 
     round("SUPPLY_TRANSIT_QTY"           ,     2),   
     round("EXPIRED_QTY"           ,       2), 
     round("BLOCKED_QTY"           ,       2), 
     round("PA_EFF_WEEK_QTY"           ,       2), 
     round("PA_BALANCE_QTY"           ,       2), 
     round("VIRTUAL_STOCK_QTY"           ,       2), 
     round("DEMAND_WO_QTY"           ,       2), 
     round("PRODUCTION_WO_QTY"           ,       2), 
     round("PRODUCTION_PLANNED_WO_QTY"           ,       2), 
     round("DEMAND_PLANNED_BATCH_WO_QTY"           ,       2), 
     round("DEMAND_UNPLANNED_BATCH_WO_QTY"           ,       2), 
     round("SUPPLY_TRANS_JOURNAL_QTY"           ,       2), 
     round("DEMAND_TRANS_JOURNAL_QTY"           ,       2), 
     round("DEMAND_PLANNED_TRANS_QTY"           ,       2), 
     round("SUPPLY_STOCK_JOURNAL_QTY"           ,      2),  
     round("DEMAND_STOCK_JOURNAL_QTY"           , 2),       
     round("SUPPLY_PO_QTY"           ,       2), 
     round("SUPPLY_PLANNED_PO_QTY"           ,       2), 
     round("SUPPLY_PO_TRANSFER_QTY"           ,       2), 
     round("SUPPLY_PO_RETURN_QTY"           ,       2), 
     round("SALES_ORDER_QTY"           ,       2), 
     round("RETURN_SALES_ORDER_QTY"           ,       2), 
     round("MINIMUM_STOCK_QTY"           ,       2), 
     round("WEEK_END_STOCK"           ,       2), 
     "CURRENT_WEEK_FLAG"           ,       
     round("PERIOD_END_FIRM_PURCHASE_QTY"           ,  2),      
    "BASE_CURRENCY"           ,      
    "PHI_CURRENCY"           ,      
    "PCOMP_CURRENCY",
      --"OC_BASE_ITEM_UNIT_PRIM_COST"   --commenting these as there are dups in the item cost if lookup done only on item and variant code,dbt code adjusted to always pick the latest row,its random in iics        ,      
    --"OC_CORP_ITEM_UNIT_PRIM_COST"           ,      
    --"OC_PCOMP_ITEM_UNIT_PRIM_COST"           ,      
    "AGREEMENT_FLAG"           ,      
    "SUPPLY_STOCK_ADJ_QTY"           ,      
    "DEMAND_STOCK_ADJ_QTY"      

from {{ref('fct_wbx_mfg_pct_weekly_agg')}} where snapshot_Date=current_Date()


),

a_intersect_b as (

    select * from a
    

    intersect


    select * from b

),

a_except_b as (

    select * from a
    

    except


    select * from b

),

b_except_a as (

    select * from b
    

    except


    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

final as (
    
    select * from all_records
    where not (in_a and in_b)
    order by  in_a desc, in_b desc

)

select * from final order by 1,2,3,4,5,6,7,8,9