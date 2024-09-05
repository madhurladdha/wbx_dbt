{{ config(enabled=false, severity="warn") }}

/*
{% set a_relation = ref("conv_fct_wbx_mfg_wo_gl_agg") %}

{% set b_relation = ref("fct_wbx_mfg_wo_gl_agg") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=["load_date", "update_date"],
        primary_key="unique_key",
        summarize=true,
    )
}}
*/
with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "DOCUMENT_COMPANY"           ,      
    "VOUCHER"           ,      
    "JOURNAL_NUMBER"           ,      
    "WORK_ORDER_NUMBER"           ,      
    "WO_SRC_ITEM_IDENTIFIER"           ,      
    "WO_SRC_VARIANT_CODE"           ,      
    "GL_DATE"           ,      
    "WO_STOCK_SITE"           ,      
    "A550010_GL_AMOUNT"           ,      
    "A550015_GL_AMOUNT"           ,      
    "A510045_GL_AMOUNT"           ,      
    "A718020_GL_AMOUNT"           ,      
    "A718040_GL_AMOUNT"           ,      
    "A718060_GL_AMOUNT"           ,      
    "SERVICE_TRANSACTION_AMT"           ,      
    "TRANSACTION_CURRENCY"           ,      
    ROUND("RECEIPE_VALUE",2)           ,      
    ROUND("ACTUAL_TRANSACTION_AMT",3)           ,      
    ROUND("PERFECTION_AMT",3)           ,      
    ROUND("STANDARD_AMT",3)          ,      
    "PRODUCED_QTY"           ,      
    "BULK_ORDER_FLAG"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "PRICE_VARIANCE_AMOUNT"           ,      
    "ITEM_MODEL_GROUP"           ,      
    "UNIQUE_KEY"      
  

from {{ref('conv_fct_wbx_mfg_wo_gl_agg')}}


),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "DOCUMENT_COMPANY"           ,      
    "VOUCHER"           ,      
    "JOURNAL_NUMBER"           ,      
    "WORK_ORDER_NUMBER"           ,      
    "WO_SRC_ITEM_IDENTIFIER"           ,      
    "WO_SRC_VARIANT_CODE"           ,      
    "GL_DATE"           ,      
    "WO_STOCK_SITE"           ,      
    "A550010_GL_AMOUNT"           ,      
    "A550015_GL_AMOUNT"           ,      
    "A510045_GL_AMOUNT"           ,      
    "A718020_GL_AMOUNT"           ,      
    "A718040_GL_AMOUNT"           ,      
    "A718060_GL_AMOUNT"           ,      
    "SERVICE_TRANSACTION_AMT"           ,      
    "TRANSACTION_CURRENCY"           ,      
    ROUND("RECEIPE_VALUE",2)           ,      
    ROUND("ACTUAL_TRANSACTION_AMT",3)           ,      
    ROUND("PERFECTION_AMT",3)           ,      
    ROUND("STANDARD_AMT",3)           ,      
    "PRODUCED_QTY"           ,      
    "BULK_ORDER_FLAG"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "PRICE_VARIANCE_AMOUNT"           ,      
    "ITEM_MODEL_GROUP"           ,      
    "UNIQUE_KEY"      
  

from {{ref('fct_wbx_mfg_wo_gl_agg')}}


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
