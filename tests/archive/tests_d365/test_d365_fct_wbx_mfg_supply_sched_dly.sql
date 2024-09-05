{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 


with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SNAPSHOT_DATE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "VARIANT_CODE"           ,      
    "TRANSACTION_DATE"           ,      
    "SOURCE_COMPANY_CODE"           ,      
    "SOURCE_SITE_CODE"           ,      
    "PLAN_VERSION"           ,      
    "ITEM_GUID"           ,      
    "BUSINESS_UNIT_ADDRESS_GUID"           ,      
    "TRANSACTION_QUANTITY"           ,      
    "ORIGINAL_QUANTITY"           ,      
    "ONHAND_QTY"           ,      
    "DEMAND_TRANSIT_QTY"           ,      
    "SUPPLY_TRANSIT_QTY"           ,      
    "EXPIRED_QTY"           ,      
    "BLOCKED_QTY"           ,      
    "DEMAND_WO_QTY"           ,      
    "PRODUCTION_WO_QTY"           ,      
    "PRODUCTION_PLANNED_WO_QTY"           ,      
    "DEMAND_PLANNED_BATCH_WO_QTY"           ,      
    "PROD_PLANNED_BATCH_WO_QTY"           ,      
    "SUPPLY_TRANS_JOURNAL_QTY"           ,      
    "DEMAND_TRANS_JOURNAL_QTY"           ,      
    "DEMAND_PLANNED_TRANS_QTY"           ,      
    "SUPPLY_STOCK_JOURNAL_QTY"           ,      
    "DEMAND_STOCK_JOURNAL_QTY"           ,      
    "SUPPLY_PLANNED_PO_QTY"           ,      
    "SUPPLY_PO_QTY"           ,      
    "SUPPLY_PO_TRANSFER_QTY"           ,      
    "SUPPLY_PO_RETURN_QTY"           ,      
    "SALES_ORDER_QTY"           ,      
    "RETURN_SALES_ORDER_QTY"           ,      
    "MINIMUM_STOCK_QTY"           ,      
    "DEMAND_UNPLANNED_BATCH_WO_QTY"           ,      
    "UNIQUE_KEY"      
  

from {{ ref('fct_wbx_mfg_supply_sched_dly') }}
  where SOURCE_COMPANY_CODE in ('WBX') and SNAPSHOT_DATE=
(select max(snapshot_date) from {{ ref('fct_wbx_mfg_supply_sched_dly') }})

),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SNAPSHOT_DATE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "VARIANT_CODE"           ,      
    "TRANSACTION_DATE"           ,      
    "SOURCE_COMPANY_CODE"           ,      
    "SOURCE_SITE_CODE"           ,      
    "PLAN_VERSION"           ,      
    "ITEM_GUID"           ,      
    "BUSINESS_UNIT_ADDRESS_GUID"           ,      
    "TRANSACTION_QUANTITY"           ,      
    "ORIGINAL_QUANTITY"           ,      
    "ONHAND_QTY"           ,      
    "DEMAND_TRANSIT_QTY"           ,      
    "SUPPLY_TRANSIT_QTY"           ,      
    "EXPIRED_QTY"           ,      
    "BLOCKED_QTY"           ,      
    "DEMAND_WO_QTY"           ,      
    "PRODUCTION_WO_QTY"           ,      
    "PRODUCTION_PLANNED_WO_QTY"           ,      
    "DEMAND_PLANNED_BATCH_WO_QTY"           ,      
    "PROD_PLANNED_BATCH_WO_QTY"           ,      
    "SUPPLY_TRANS_JOURNAL_QTY"           ,      
    "DEMAND_TRANS_JOURNAL_QTY"           ,      
    "DEMAND_PLANNED_TRANS_QTY"           ,      
    "SUPPLY_STOCK_JOURNAL_QTY"           ,      
    "DEMAND_STOCK_JOURNAL_QTY"           ,      
    "SUPPLY_PLANNED_PO_QTY"           ,      
    "SUPPLY_PO_QTY"           ,      
    "SUPPLY_PO_TRANSFER_QTY"           ,      
    "SUPPLY_PO_RETURN_QTY"           ,      
    "SALES_ORDER_QTY"           ,      
    "RETURN_SALES_ORDER_QTY"           ,      
    "MINIMUM_STOCK_QTY"           ,      
    "DEMAND_UNPLANNED_BATCH_WO_QTY"           ,      
    "UNIQUE_KEY"      
  

from wbx_prod.fact.fct_wbx_mfg_supply_sched_dly
    where SOURCE_COMPANY_CODE in ('WBX') and snapshot_date=
(select max(snapshot_date) from wbx_prod.fact.fct_wbx_mfg_supply_sched_dly)

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
    order by UNIQUE_KEY,  in_a desc, in_b desc

)

select * from final
