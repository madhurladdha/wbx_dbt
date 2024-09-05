{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 

--this code base is manually compiled. Since, model is incremental in nature and we just need to compare latest snapshot.

with a as (
select
    "SOURCE_SYSTEM"           ,      
    "WORK_ORDER_NUMBER"           ,      
    "SNAPSHOT_VERSION"           ,      
    "SOURCE_ORDER_TYPE_CODE"           ,      
    "ORDER_TYPE_DESC"           ,      
    "RELATED_DOCUMENT_TYPE"           ,      
    "RELATED_DOCUMENT_NUMBER"           ,      
    "RELATED_LINE_NUMBER"           ,      
    "PRIORITY_CODE"           ,      
    "PRIORITY_DESC"           ,      
    "DESCRIPTION"           ,      
    "COMPANY_CODE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "BUSINESS_UNIT_ADDRESS_GUID"           ,      
    "STATUS_CODE"           ,      
    "STATUS_DESC"           ,      
    "STATUS_CHANGE_DATE"           ,      
    "SOURCE_CUSTOMER_CODE"           ,      
    "CUSTOMER_ADDRESS_NUMBER_GUID"           ,      
    "WO_CREATOR_ADD_NUMBER"           ,      
    "MANAGER_ADD_NUMBER"           ,      
    "SUPERVISOR_ADD_NUMBER"           ,      
    "PLANNED_COMPLETION_DATE"           ,      
    "ORDER_DATE"           ,      
    "PLANNED_START_DATE"           ,      
    "REQUESTED_DATE"           ,      
    "ASSIGNED_DATE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "ITEM_GUID"           ,      
    "SCHEDULED_QTY"           ,      
    "TRANSACTION_UOM"           ,      
    "PRIMARY_UOM"           ,      
    "TRAN_PRIM_CONV_FACTOR"           ,      
    "TRAN_LB_CONV_FACTOR"           ,      
    "SCHEDULED_LB_QTY"           ,      
    "WORK_CENTER_CODE"           ,      
    "WORK_CENTER_DESC"           ,      
    "SCHEDULED_KG_QTY"           ,      
    "TRAN_KG_CONV_FACTOR"           ,      
    "SOURCE_BOM_IDENTIFIER"           ,      
    "GL_DATE"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    "ITEM_MODEL_GROUP"           ,      
    "VOUCHER"           ,      
    "PRODUCT_CLASS"           ,      
    "SITE"           ,      
    "UNIQUE_KEY"      
from fct_wbx_mfg_wo_sched_snapshot
  where COMPANY_CODE in ('WBX')
  and SNAPSHOT_DATE=(select max(SNAPSHOT_DATE) from fct_wbx_mfg_wo_sched_snapshot)

),

b as (
select
    "SOURCE_SYSTEM"           ,      
    "WORK_ORDER_NUMBER"           ,      
    "SNAPSHOT_VERSION"           ,      
    "SOURCE_ORDER_TYPE_CODE"           ,      
    "ORDER_TYPE_DESC"           ,      
    "RELATED_DOCUMENT_TYPE"           ,      
    "RELATED_DOCUMENT_NUMBER"           ,      
    "RELATED_LINE_NUMBER"           ,      
    "PRIORITY_CODE"           ,      
    "PRIORITY_DESC"           ,      
    "DESCRIPTION"           ,      
    "COMPANY_CODE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "BUSINESS_UNIT_ADDRESS_GUID"           ,      
    "STATUS_CODE"           ,      
    "STATUS_DESC"           ,      
    "STATUS_CHANGE_DATE"           ,      
    "SOURCE_CUSTOMER_CODE"           ,      
    "CUSTOMER_ADDRESS_NUMBER_GUID"           ,      
    "WO_CREATOR_ADD_NUMBER"           ,      
    "MANAGER_ADD_NUMBER"           ,      
    "SUPERVISOR_ADD_NUMBER"           ,      
    "PLANNED_COMPLETION_DATE"           ,      
    "ORDER_DATE"           ,      
    "PLANNED_START_DATE"           ,      
    "REQUESTED_DATE"           ,      
    "ASSIGNED_DATE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "ITEM_GUID"           ,      
    "SCHEDULED_QTY"           ,      
    "TRANSACTION_UOM"           ,      
    "PRIMARY_UOM"           ,      
    "TRAN_PRIM_CONV_FACTOR"           ,      
    "TRAN_LB_CONV_FACTOR"           ,      
    "SCHEDULED_LB_QTY"           ,      
    "WORK_CENTER_CODE"           ,      
    "WORK_CENTER_DESC"           ,      
    "SCHEDULED_KG_QTY"           ,      
    "TRAN_KG_CONV_FACTOR"           ,      
    "SOURCE_BOM_IDENTIFIER"           ,      
    "GL_DATE"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    "ITEM_MODEL_GROUP"           ,      
    "VOUCHER"           ,      
    "PRODUCT_CLASS"           ,      
    "SITE"           ,      
    "UNIQUE_KEY"      
from wbx_prod.fact.fct_wbx_mfg_wo_sched_snapshot
    where COMPANY_CODE in ('WBX')
  and SNAPSHOT_DATE=(select max(SNAPSHOT_DATE) from wbx_prod.fact.fct_wbx_mfg_wo_sched_snapshot)

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
    order by in_a desc, in_b desc

)
select * from final

