{{
    config(
        materialized=env_var("DBT_MAT_VIEW"),
        tags=["sales", "performance", "sales_performance_lite"],
    )
}}

with
    cte_hirerchy as (select distinct
            source_system as source_system,
            company_code,
            node_1 as market_code,
            desc_1 as market,
            node_2 as sub_market_code,
            desc_2 as sub_market,
            node_3 as trade_class_code,
            desc_3 as trade_class,
            node_4 as trade_group_code,
            desc_4 as trade_group,
            node_5 as trade_type_code,
            desc_5 as trade_type from {{ ref("xref_wbx_hierarchy") }}  
            where
            source_system = '{{env_var('DBT_SOURCE_SYSTEM')}}'
            and hier_name = 'CUSTOMER-SALES'),
    cte_customer_ext as (select * from {{ ref("dim_wbx_customer_ext") }})

select
    cust_h.company_code as company_code,
    cust_h.market_code as market_code,
    cust_h.market as market,
    cust_e.market_code_seq as market_code_seq,
    cust_h.sub_market_code as sub_market_code,
    cust_h.sub_market as sub_market,
    cust_e.sub_market_code_seq as sub_market_code_seq,
    cust_h.trade_class_code as trade_class_code,
    cust_h.trade_class as trade_class,
    cust_e.trade_class_seq as trade_class_seq,
    cust_h.trade_group_code as trade_group_code,
    cust_h.trade_group as trade_group,
    cust_e.trade_group_seq as trade_group_seq,
    cust_h.trade_type_code as trade_type_code,
    cust_h.trade_type as trade_type,
    cust_e.trade_type_seq as trade_type_seq,
    cust_e.trade_sector_code as trade_sector_code,
    cust_e.trade_sector_desc as trade_sector_desc,
    cust_e.trade_sector_seq as trade_sector_seq
from
    (select * from (
        select distinct
            source_system,
            company_code,
            market_code,
            market,
            sub_market_code,
            sub_market,
            trade_class_code,
            trade_class,
            trade_group_code,
            trade_group,
            trade_type_code,
            trade_type,
            row_number() over (PARTITION BY TRADE_TYPE_CODE,company_code
                                          order by MARKET_CODE,SUB_MARKET_CODE,TRADE_CLASS_CODE,TRADE_GROUP_CODE desc ) as  ROWNUM
        from cte_hirerchy
    ) where ROWNUM=1 ) cust_h
join
    (
        select * from (
            select
            source_system,
            company_code,
            trade_sector_code as trade_sector_code,
            trade_sector_desc as trade_sector_desc,
            trade_sector_seq,
            trim(market_code) as market_code,
            market_code_seq,
            trim(sub_market_code) as sub_market_code,
            sub_market_code_seq,
            trim(trade_class_code) as trade_class_code,
            trade_class_seq,
            trim(trade_group_code) as trade_group_code,
            trade_group_seq,
            trim(trade_type_code) as trade_type_code,
            trade_type_seq,
            row_number() over (PARTITION BY TRADE_TYPE_CODE,company_code
                                          order by TRADE_SECTOR_CODE,MARKET_CODE,SUB_MARKET_CODE,TRADE_CLASS_CODE,TRADE_GROUP_CODE ) as  ROWNUM
        from cte_customer_ext
        where
            (
                trim(trade_type_code) is not null
                and trim(trade_type_code) != ''
                and source_system_address_number not like '%-0000'
            ) ) where ROWNUM=1
    ) cust_e
    on cust_h.source_system = cust_e.source_system
    and trim(cust_h.trade_type_code) = trim(cust_e.trade_type_code)
    and trim(cust_h.trade_group_code) = trim(cust_e.trade_group_code)
    and trim(cust_h.trade_class_code) = trim(cust_e.trade_class_code)
    and trim(cust_h.sub_market_code) = trim(cust_e.sub_market_code)
    and trim(cust_h.market_code) = trim(cust_e.market_code) 
    and trim(cust_h.company_code)=trim(cust_e.company_code) 
