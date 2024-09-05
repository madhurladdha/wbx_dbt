{{ config(enabled=false, severity="warn") }}
/*
{% set a_relation = ref("conv_fct_wbx_mfg_yield_agg") %}

{% set b_relation = ref("fct_wbx_mfg_yield_agg") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=["load_date", "update_date"],
        primary_key="unique_key",
        summarize=false,
    )
}}
*/

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
    "WO_SRC_VARIANT_CODE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "COMPANY_CODE"           ,      
    "COMP_TRANSACTION_UOM"           ,      
    "TRANSACTION_CURRENCY"           ,      
    ROUND("ACTUAL_TRANSACTION_QTY",4)           ,      
    ROUND("COMP_STANDARD_QUANTITY",4)           ,      
    ROUND("COMP_PERFECTION_QUANTITY",4)           ,      
    ROUND("COMP_SCRAP_PERCENT",4)           ,      
    "ITEM_MATCH_BOM_FLAG"           ,      
    ROUND("TRANSACTION_AMT",4)           ,      
    ROUND("STOCK_ADJ_QTY",4)           ,      
    "PRODUCT_CLASS"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    ROUND("TRANDT_ACTUAL_AMOUNT",4)           ,      
    ROUND("GLDT_ACTUAL_AMOUNT",4)           ,      
    ROUND("STANDARD_AMOUNT",4)           ,      
    ROUND("PERFECTION_AMOUNT",4)           ,      
    ROUND("GLDT_STOCK_ADJ_AMOUNT",4)           ,      
    "COMP_ITEM_MODEL_GROUP"           ,      
    "WO_ITEM_MODEL_GROUP"           ,      
    "WO_STOCK_SITE"           ,      
    "FLAG"           ,      
    "GL_DATE"           ,      
    "UNIQUE_KEY"      
  

from {{ref("conv_fct_wbx_mfg_yield_agg") }}


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
    "WO_SRC_VARIANT_CODE"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "COMPANY_CODE"           ,      
    "COMP_TRANSACTION_UOM"           ,      
    "TRANSACTION_CURRENCY"           ,      
    ROUND("ACTUAL_TRANSACTION_QTY",4)           ,      
    ROUND("COMP_STANDARD_QUANTITY",4)           ,      
    ROUND("COMP_PERFECTION_QUANTITY",4)           ,      
    ROUND("COMP_SCRAP_PERCENT",4)           ,      
    "ITEM_MATCH_BOM_FLAG"           ,      
    ROUND("TRANSACTION_AMT",4)           ,      
    ROUND("STOCK_ADJ_QTY",4)           ,      
    "PRODUCT_CLASS"           ,      
    "CONSOLIDATED_BATCH_ORDER"           ,      
    "BULK_FLAG"           ,      
    ROUND("TRANDT_ACTUAL_AMOUNT",4)           ,      
    ROUND("GLDT_ACTUAL_AMOUNT",4)           ,      
    ROUND("STANDARD_AMOUNT",4)           ,      
    ROUND("PERFECTION_AMOUNT",4)           ,      
    ROUND("GLDT_STOCK_ADJ_AMOUNT",4)           ,      
    "COMP_ITEM_MODEL_GROUP"           ,      
    "WO_ITEM_MODEL_GROUP"           ,      
    "WO_STOCK_SITE"           ,      
    "FLAG"           ,      
    "GL_DATE"           ,      
    "UNIQUE_KEY"      
  

from {{ref("fct_wbx_mfg_yield_agg") }}


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