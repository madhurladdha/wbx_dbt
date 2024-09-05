{{ 
config(
    enabled=false, 
    severity="warn",
    tags=["manufacturing", "cbom","mfg_cbom","sales", "terms","sls_terms"]
)
}}

/* Used a more hard-coded version of this compare test so that we can round some of the measure columns for reasonable comparisons.
*/

/*
{% set old_etl_relation = ref("conv_mfg_fct_wbx_cbom") %}
{% set dbt_relation = ref("fct_wbx_mfg_cbom") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        exclude_columns=[
            "item_guid",
            "comp_src_item_guid",
            "root_src_item_guid",
            "load_date",
            "update_date"
        ],
        primary_key="UNIQUE_KEY",
        summarize=true
    )
}}
*/


with a as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "ACTIVE_FLAG"           ,      
    "VERSION_ID"           ,      
    "EFF_DATE"           ,      
    "CREATION_DATE_TIME"           ,      
    "EXPIR_DATE"           ,      
    "SOURCE_UPDATED_DATETIME"           ,      
    "TRANSACTION_CURRENCY"           ,      
    "TRANSACTION_UOM"           ,      
    "ROOT_COMPANY_CODE"           ,      
    "ROOT_SRC_ITEM_IDENTIFIER"           ,      
    "ROOT_SRC_VARIANT_CODE"           ,      
    "COMP_SRC_ITEM_IDENTIFIER"           ,      
    "COMP_SRC_VARIANT_CODE"           ,     
    ROUND("COMP_CONSUMPTION_QTY",1) AS    "COMP_CONSUMPTION_QTY"        ,      
    --"COMP_CONSUMPTION_QTY"           ,      
    "COMP_CONSUMPTION_UNIT"           ,      
    "COMP_COST_PRICE"           ,      
    "COMP_COST_PRICE_UNIT"           ,      
    ROUND("COMP_ITEM_UNIT_COST",6) AS    "COMP_ITEM_UNIT_COST"        ,      
    "COMP_BOM_LEVEL"           ,      
    "COMP_CALCTYPE_DESC"           ,      
    "COMP_COST_GROUP_ID"           ,      
    "PARENT_ITEM_INDICATOR"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "SOURCE_BOM_PATH"           ,      
    "STOCK_SITE"           ,      
    --"ROOT_SRC_UNIT_PRICE"           , 
    ROUND("ROOT_SRC_UNIT_PRICE",6) AS    "ROOT_SRC_UNIT_PRICE"        ,  
    "UNIQUE_KEY"      
  

from {{ ref("conv_mfg_fct_wbx_cbom") }}


),

b as (

    
select
  
    "SOURCE_SYSTEM"           ,      
    "ACTIVE_FLAG"           ,      
    "VERSION_ID"           ,      
    "EFF_DATE"           ,      
    "CREATION_DATE_TIME"           ,      
    "EXPIR_DATE"           ,      
    "SOURCE_UPDATED_DATETIME"           ,      
    "TRANSACTION_CURRENCY"           ,      
    "TRANSACTION_UOM"           ,      
    "ROOT_COMPANY_CODE"           ,      
    "ROOT_SRC_ITEM_IDENTIFIER"           ,      
    "ROOT_SRC_VARIANT_CODE"           ,      
    "COMP_SRC_ITEM_IDENTIFIER"           ,      
    "COMP_SRC_VARIANT_CODE"           ,      
    ROUND("COMP_CONSUMPTION_QTY",1) AS    "COMP_CONSUMPTION_QTY"        ,      
    --"COMP_CONSUMPTION_QTY"           ,     
    "COMP_CONSUMPTION_UNIT"           ,      
    "COMP_COST_PRICE"           ,      
    "COMP_COST_PRICE_UNIT"           ,      
    ROUND("COMP_ITEM_UNIT_COST",6) AS    "COMP_ITEM_UNIT_COST"        ,      
    "COMP_BOM_LEVEL"           ,      
    "COMP_CALCTYPE_DESC"           ,      
    "COMP_COST_GROUP_ID"           ,      
    "PARENT_ITEM_INDICATOR"           ,      
    "SOURCE_BUSINESS_UNIT_CODE"           ,      
    "SOURCE_BOM_PATH"           ,      
    "STOCK_SITE"           ,      
    --"ROOT_SRC_UNIT_PRICE"           , 
    ROUND("ROOT_SRC_UNIT_PRICE",6) AS    "ROOT_SRC_UNIT_PRICE"        ,  
    "UNIQUE_KEY"   
  

from {{ ref("fct_wbx_mfg_cbom") }}


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
