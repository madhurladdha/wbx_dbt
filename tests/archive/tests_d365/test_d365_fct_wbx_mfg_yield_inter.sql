{{ config( 
  enabled=false, 
  severity = 'warn', 
  warn_if = '>0' 
) }} 

--unique_key is missing from this fact model. Hence, compiled this test model.

with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "COMP_STOCK_SITE"           ,      
    "FINANCIAL_SITE"           ,      
    "VOUCHER"           ,      
    "WORK_ORDER_NUMBER"           ,      
    "COMP_SRC_ITEM_IDENTIFIER"           ,      
    "COMP_SRC_VARIANT_CODE"           ,      
    "TRANSACTION_DATE"           ,      
    "COMP_ITEM_TYPE"           ,      
    "SOURCE_BOM_IDENTIFIER"           ,      
    "WO_SRC_ITEM_IDENTIFIER"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "COMPANY_CODE"           ,      
    "COMP_TRANSACTION_UOM"           ,      
    "TRANSACTION_CURRENCY"           ,      
    "WO_SRC_VARIANT_CODE"           ,      
    "ACTUAL_TRANSACTION_QTY"           ,      
    "COMP_STANDARD_QUANTITY"           ,      
    "COMP_PERFECTION_QUANTITY"           ,      
    "COMP_SCRAP_PERCENT"           ,      
    "ITEM_MATCH_BOM_FLAG"           ,      
    "TRANSACTION_AMT"           ,      
    "STOCK_ADJ_QTY"           ,      
    "PRODUCT_CLASS"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    "GL_DATE"           ,      
    "GLDT_UNIT_PRICE"           ,      
    "TRANDT_UNIT_PRICE"           ,      
    "GLDT_ACTUAL_AMOUNT"           ,      
    "TRANDT_ACTUAL_AMOUNT"           ,      
    "STANDARD_AMOUNT"           ,      
    "GLDT_STOCK_ADJ_AMOUNT"           ,      
    "PERFECTION_AMOUNT"           ,      
    "COMP_ITEM_MODEL_GROUP"           ,      
    "WO_ITEM_MODEL_GROUP"           ,      
    "WO_STOCK_SITE"           ,      
    "FLAG"      
  

from {{ ref('fct_wbx_mfg_yield_inter') }}
  where COMPANY_CODE in ('WBX')

),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "COMP_STOCK_SITE"           ,      
    "FINANCIAL_SITE"           ,      
    "VOUCHER"           ,      
    "WORK_ORDER_NUMBER"           ,      
    "COMP_SRC_ITEM_IDENTIFIER"           ,      
    "COMP_SRC_VARIANT_CODE"           ,      
    "TRANSACTION_DATE"           ,      
    "COMP_ITEM_TYPE"           ,      
    "SOURCE_BOM_IDENTIFIER"           ,      
    "WO_SRC_ITEM_IDENTIFIER"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "COMPANY_CODE"           ,      
    "COMP_TRANSACTION_UOM"           ,      
    "TRANSACTION_CURRENCY"           ,      
    "WO_SRC_VARIANT_CODE"           ,      
    "ACTUAL_TRANSACTION_QTY"           ,      
    "COMP_STANDARD_QUANTITY"           ,      
    "COMP_PERFECTION_QUANTITY"           ,      
    "COMP_SCRAP_PERCENT"           ,      
    "ITEM_MATCH_BOM_FLAG"           ,      
    "TRANSACTION_AMT"           ,      
    "STOCK_ADJ_QTY"           ,      
    "PRODUCT_CLASS"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    "GL_DATE"           ,      
    "GLDT_UNIT_PRICE"           ,      
    "TRANDT_UNIT_PRICE"           ,      
    "GLDT_ACTUAL_AMOUNT"           ,      
    "TRANDT_ACTUAL_AMOUNT"           ,      
    "STANDARD_AMOUNT"           ,      
    "GLDT_STOCK_ADJ_AMOUNT"           ,      
    "PERFECTION_AMOUNT"           ,      
    "COMP_ITEM_MODEL_GROUP"           ,      
    "WO_ITEM_MODEL_GROUP"           ,      
    "WO_STOCK_SITE"           ,      
    "FLAG"      
  

from wbx_prod.fact.fct_wbx_mfg_yield_inter
    where COMPANY_CODE in ('WBX')

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
    order by   in_a desc, in_b desc

)

select * from final
