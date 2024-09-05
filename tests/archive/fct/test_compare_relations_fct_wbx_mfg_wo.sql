{{ config(enabled=false, severity="warn") }}

with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "WORK_ORDER_NUMBER"           ,      
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
    "STATUS_CODE"           ,      
    "STATUS_DESC"           ,      
    "STATUS_CHANGE_DATE"           ,      
    "SOURCE_CUSTOMER_CODE"           ,      
    "WO_CREATOR_ADD_NUMBER"           ,      
    "MANAGER_ADD_NUMBER"           ,      
    "SUPERVISOR_ADD_NUMBER"           ,      
    "PLANNED_COMPLETION_DATE"           ,      
    "ORDER_DATE"           ,      
    "PLANNED_START_DATE"           ,      
    "REQUESTED_DATE"           ,      
    "ACTUAL_START_DATE"           ,      
    "ACTUAL_COMPLETION_DATE"           ,      
    "ASSIGNED_DATE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "SCHEDULED_QTY"           ,      
    "CANCELLED_QTY"           ,      
    "PRODUCED_QTY"           ,      
    "TRANSACTION_UOM"           ,      
    "PRIMARY_UOM"           ,      
    "TRAN_PRIM_CONV_FACTOR"           ,      
    "TRAN_LB_CONV_FACTOR"           ,      
    ROUND("SCHEDULED_LB_QTY",4)           ,      
    ROUND("PRODUCED_LB_QTY",4)           ,      
    "SCHEDULED_SNAPSHOT_DATE"           ,      
    "SCHEDULED_SNAPSHOT_VERSION"           ,      
    "ORIG_PLANNED_COMPLETION_DATE"           ,      
    "ORIG_PLANNED_START_DATE"           ,      
    "ORIG_SCHEDULED_QTY"           ,      
    "WORK_CENTER_CODE"           ,      
    "WORK_CENTER_DESC"           ,      
    "ORIG_SCHEDULED_KG_QTY"           ,      
    "TRAN_KG_CONV_FACTOR"           ,      
    "PRODUCED_KG_QTY"           ,      
    "SCHEDULED_KG_QTY"           ,      
    "CTP_TARGET_PERCENT"           ,      
    "PTP_TARGET_PERCENT"           ,      
    "SOURCE_BOM_IDENTIFIER"           ,      
    "GL_DATE"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    "ITEM_MODEL_GROUP"           ,      
    "VOUCHER"           ,      
    "PRODUCT_CLASS"           ,      
    "SITE"           ,      
    "UNIQUE_KEY"      
  

from {{ref('conv_fct_wbx_mfg_wo')}}


),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "WORK_ORDER_NUMBER"           ,      
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
    "STATUS_CODE"           ,      
    "STATUS_DESC"           ,      
    "STATUS_CHANGE_DATE"           ,      
    "SOURCE_CUSTOMER_CODE"           ,      
    "WO_CREATOR_ADD_NUMBER"           ,      
    "MANAGER_ADD_NUMBER"           ,      
    "SUPERVISOR_ADD_NUMBER"           ,      
    "PLANNED_COMPLETION_DATE"           ,      
    "ORDER_DATE"           ,      
    "PLANNED_START_DATE"           ,      
    "REQUESTED_DATE"           ,      
    "ACTUAL_START_DATE"           ,      
    "ACTUAL_COMPLETION_DATE"           ,      
    "ASSIGNED_DATE"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "SCHEDULED_QTY"           ,      
    "CANCELLED_QTY"           ,      
    "PRODUCED_QTY"           ,      
    "TRANSACTION_UOM"           ,      
    "PRIMARY_UOM"           ,      
    "TRAN_PRIM_CONV_FACTOR"           ,      
    "TRAN_LB_CONV_FACTOR"           ,      
    ROUND("SCHEDULED_LB_QTY",4)           ,      
    ROUND("PRODUCED_LB_QTY",4)           ,      
    "SCHEDULED_SNAPSHOT_DATE"           ,      
    "SCHEDULED_SNAPSHOT_VERSION"           ,      
    "ORIG_PLANNED_COMPLETION_DATE"           ,      
    "ORIG_PLANNED_START_DATE"           ,      
    "ORIG_SCHEDULED_QTY"           ,      
    "WORK_CENTER_CODE"           ,      
    "WORK_CENTER_DESC"           ,      
    "ORIG_SCHEDULED_KG_QTY"           ,      
    "TRAN_KG_CONV_FACTOR"           ,      
    "PRODUCED_KG_QTY"           ,      
    "SCHEDULED_KG_QTY"           ,      
    "CTP_TARGET_PERCENT"           ,      
    "PTP_TARGET_PERCENT"           ,      
    "SOURCE_BOM_IDENTIFIER"           ,      
    "GL_DATE"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    "ITEM_MODEL_GROUP"           ,      
    "VOUCHER"           ,      
    "PRODUCT_CLASS"           ,      
    "SITE"           ,      
    "UNIQUE_KEY"      
  

from {{ref('fct_wbx_mfg_wo')}}


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
    order by unique_key,  in_a desc, in_b desc

)

select * from final