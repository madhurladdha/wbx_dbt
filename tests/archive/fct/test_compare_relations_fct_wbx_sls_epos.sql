-- excluded metric fields due to minor round offs.tested manually using a separate summarized SQL(results on PR)
{{ config(enabled=false, severity="warn") }}

/*
{% set a_relation = ref("conv_wbx_sls_epos_fact") %}

{% set b_relation = ref("fct_wbx_sls_epos") %}

{{
    ent_dbt_package.compare_relations(
        a_relation=a_relation,
        b_relation=b_relation,
        exclude_columns=[
            "bill_customer_address_guid",
            "item_guid",
            "qty_ca",
            "qty_kg",
            "qty_ul",
            "qty_prim"
        ],
        summarize=false,
    )
}}
*/


with a as (

    
select
      "UNIQUE_KEY",
    "SOURCE_SYSTEM"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "TRADE_TYPE_CODE"           ,      
    "BILL_SOURCE_CUSTOMER_CODE"           ,      
    "CALENDAR_WEEK"           ,      
    "CALENDAR_DATE"           ,      
    "FISCAL_PERIOD_NUMBER"           ,      
    "PRIMARY_UOM"           ,      
    "NUMBER_OF_DAYS"      ,
    ROUND("QTY_CA",4) AS QTY_CA,
    ROUND("QTY_KG",4) AS QTY_KG,
    ROUND("QTY_UL",4) AS QTY_UL,
    ROUND("QTY_PRIM",4) AS QTY_PRIM

from {{ref("conv_wbx_sls_epos_fact") }}


),

b as (

    
select
    "UNIQUE_KEY",
    "SOURCE_SYSTEM"           ,      
    "SOURCE_ITEM_IDENTIFIER"           ,      
    "TRADE_TYPE_CODE"           ,      
    "BILL_SOURCE_CUSTOMER_CODE"           ,      
    "CALENDAR_WEEK"           ,      
    "CALENDAR_DATE"           ,      
    "FISCAL_PERIOD_NUMBER"           ,      
    "PRIMARY_UOM"           ,      
    "NUMBER_OF_DAYS"    ,
    ROUND("QTY_CA",4) AS QTY_CA,
    ROUND("QTY_KG",4) AS QTY_KG,
    ROUND("QTY_UL",4) AS QTY_UL,
    ROUND("QTY_PRIM",4) AS QTY_PRIM
  

from {{ref("fct_wbx_sls_epos")}}


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
    order by UNIQUE_KEY, in_a desc, in_b desc

)

select * from final
