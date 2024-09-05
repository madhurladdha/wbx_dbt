{{
    config(
        tags = ["sls","sales","forecast","sls_forecast"]
    )
}}
with ref_hierarchy_xref as (
    select
        distinct
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
        desc_5 as trade_type
    from {{ ref('xref_wbx_hierarchy')}}
    where source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    and hier_name = 'CUSTOMER-SALES'
),
customer_ext as (
    select 
        source_system           as source_system,
        company_code,
        max(trade_sector_code)  as trade_sector_code,
        max(trade_sector_desc)  as trade_sector_desc,
        max(trade_sector_seq)   as trade_sector_seq,
        trim(market_code)       as market_code,
        market_code_seq         as market_code_seq,
        trim(sub_market_code)   as sub_market_code,
        sub_market_code_seq,
        trim(trade_class_code)  as trade_class_code,
        trade_class_seq         as trade_class_seq,
        trim(trade_group_code)  as trade_group_code,
        trade_group_seq         as trade_group_seq,
        trim(trade_type_code)   as trade_type_code,
        trade_type_seq          as trade_type_seq
    from {{ ref('dim_wbx_customer_ext')}}
    where (trim(trade_type_code) is not null and  trim(trade_type_code)!=''
    and source_system_address_number not like '%-0000')  
    group by source_system,company_code,trim(market_code),market_code_seq,trim(sub_market_code),
             sub_market_code_seq,trim(trade_class_code),trade_class_seq,trim(trade_group_code),
             trade_group_seq,trim(trade_type_code),trade_type_seq
),
source as (
    select
        cust_h.market_code          as market_code,
        cust_h.market               as market,
        cust_e.market_code_seq      as market_code_seq,
        cust_h.sub_market_code      as sub_market_code,
        cust_h.sub_market           as sub_market,
        cust_e.sub_market_code_seq  as sub_market_code_seq,
        cust_h.trade_class_code     as trade_class_code,
        cust_h.trade_class          as trade_class,
        cust_e.trade_class_seq      as trade_class_seq,
        cust_h.trade_group_code     as trade_group_code,
        cust_h.trade_group          as trade_group,
        cust_e.trade_group_seq      as trade_group_seq,
        cust_h.trade_type_code      as trade_type_code,
        cust_h.trade_type           as trade_type,
        cust_e.trade_type_seq       as trade_type_seq,
        cust_e.trade_sector_code    as trade_sector_code,
        cust_e.trade_sector_desc    as trade_sector_desc,
        cust_e.trade_sector_seq     as trade_sector_seq 
    from ref_hierarchy_xref cust_h 
    join customer_ext cust_e on cust_h.source_system = cust_e.source_system
                            and trim(cust_h.trade_type_code) = trim(cust_e.trade_type_code)
                            and trim(cust_h.trade_group_code) = trim(cust_e.trade_group_code)
                            and trim(cust_h.trade_class_code) = trim(cust_e.trade_class_code)
                            and trim(cust_h.sub_market_code) = trim(cust_e.sub_market_code)
                            and trim(cust_h.market_code) = trim(cust_e.market_code)
                            and trim(cust_h.company_code)=trim(cust_e.company_code)
),
final as (
    select
        market_code,
	    market,
	    market_code_seq,
	    sub_market_code,
	    sub_market,
	    sub_market_code_seq,
	    trade_class_code,
	    trade_class,
	    trade_class_seq,
	    trade_group_code,
	    trade_group,
	    trade_group_seq,
	    trade_type_code,
	    trade_type,
	    trade_type_seq,
	    trade_sector_code,
	    trade_sector_desc,
	    trade_sector_seq
    from source
)
select * from final
