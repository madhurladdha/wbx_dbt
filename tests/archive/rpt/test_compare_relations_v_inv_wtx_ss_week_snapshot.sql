/*Due to huge volume,Changing the test to manual to check the results for the current year*/
{{ config( 
  enabled=false, 
  snowflake_warehouse=env_var("DBT_WBX_SF_WH"),
  severity = 'warn', 
  warn_if = '>0' ,
  tags=["manufacturing", "supply_Schedule", "wbx", "weekly", "inventory"]

) }}

with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SNAPSHOT_DATE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "ITEM_TYPE"           ,      
    "ITEM_ALLOCATION_KEY"           ,      
    "VARIANT_CODE"           ,      
    "SOURCE_COMPANY_CODE"           ,      
    "SOURCE_SITE_CODE"           ,      
    "PLAN_VERSION"           ,      
    "WEEK_DESC"           ,      
    "WEEK_START_DT"           ,      
    "WEEK_END_DT"           ,      
    "CURRENT_WEEK_FLAG"           ,      
    "WEEK_START_STOCK"           ,      
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
    "SUPPLY_PO_QTY"           ,      
    "SUPPLY_PLANNED_PO_QTY"           ,      
    "SUPPLY_PO_TRANSFER_QTY"           ,      
    "SUPPLY_PO_RETURN_QTY"           ,      
    "SALES_ORDER_QTY"           ,      
    "RETURN_SALES_ORDER_QTY"           ,      
    "MINIMUM_STOCK_QTY"           ,      
    "WEEK_END_STOCK"           ,      
    "DESCRIPTION"           ,      
    "BUYER_CODE"           ,      
    "SUPPLIER_NAME"           ,      
    "PERIOD_END_FIRM_PURCHASE_QTY"           ,      
    "DEMAND_UNPLANNED_BATCH_WO_QTY"           ,      
    "TOTAL_SUPPLY_QTY"           ,      
    "TOTAL_DEMAND_QTY"           ,      
    "PRIMARY_UOM"      
  

from {{ source("FACTS_FOR_COMPARE","v_inv_wtx_ss_week_snapshot") }}
where snapshot_Date in(select max(snapshot_date) from {{ source("FACTS_FOR_COMPARE","v_inv_wtx_ss_week_snapshot") }})


),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "SNAPSHOT_DATE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "ITEM_TYPE"           ,      
    "ITEM_ALLOCATION_KEY"           ,      
    "VARIANT_CODE"           ,      
    "SOURCE_COMPANY_CODE"           ,      
    "SOURCE_SITE_CODE"           ,      
    "PLAN_VERSION"           ,      
    "WEEK_DESC"           ,      
    "WEEK_START_DT"           ,      
    "WEEK_END_DT"           ,      
    "CURRENT_WEEK_FLAG"           ,      
    "WEEK_START_STOCK"           ,      
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
    "SUPPLY_PO_QTY"           ,      
    "SUPPLY_PLANNED_PO_QTY"           ,      
    "SUPPLY_PO_TRANSFER_QTY"           ,      
    "SUPPLY_PO_RETURN_QTY"           ,      
    "SALES_ORDER_QTY"           ,      
    "RETURN_SALES_ORDER_QTY"           ,      
    "MINIMUM_STOCK_QTY"           ,      
    "WEEK_END_STOCK"           ,      
    "DESCRIPTION"           ,      
    "BUYER_CODE"           ,      
    "SUPPLIER_NAME"           ,      
    "PERIOD_END_FIRM_PURCHASE_QTY"           ,      
    "DEMAND_UNPLANNED_BATCH_WO_QTY"           ,      
    "TOTAL_SUPPLY_QTY"           ,      
    "TOTAL_DEMAND_QTY"           ,      
    "PRIMARY_UOM"      
  

from {{ ref("v_inv_wtx_ss_week_snapshot")}}
where snapshot_date in(select max(snapshot_date) from {{ ref("v_inv_wtx_ss_week_snapshot")}} )


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

select * from final