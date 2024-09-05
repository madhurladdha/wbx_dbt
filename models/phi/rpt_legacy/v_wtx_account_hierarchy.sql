{{ config(tags=["account", "hierarchy"]) }}

with ref_hierarchy_xref as (select * from {{ ref('xref_wbx_hierarchy') }}),
adr_wtx_cust_master_ext as (select * from {{ ref('dim_wbx_customer_ext') }}),
adr_customer_master_dim as (select * from {{ ref('dim_wbx_customer') }}),
adr_address_master_dim as (select * from {{ ref('dim_wbx_address') }}),
custtable as (select * from {{ ref('src_custtable') }}),
dimensionfinancialtag as (select * from {{ ref('src_dimensionfinancialtag') }}),
dimensionattributedircategory as (select * from {{ ref('src_dimensionattributedircategory') }}),
dimensionattribute as (select * from {{ ref('src_dimensionattribute') }})

select
    to_number(cust_h.node_level, 38, 0) as node_level,
    cust_h.node_1 as market_code,
    cust_h.desc_1 as market,
    cust_h.node_2 as sub_market_code,
    cust_h.desc_2 as sub_market,
    cust_h.node_3 as trade_class_code,
    cust_h.desc_3 as trade_class,
    cust_h.node_4 as trade_group_code,
    cust_h.desc_4 as trade_group,
    cust_h.node_5 as trade_type_code,
    cust_h.desc_5 as trade_type,
    cust_h.node_6 as customer_account_number,
    cust_h.desc_6 as customer_account,
    cust_h.node_7 as customer_branch_number,
    cust_h.desc_7 as customer_branch,

    nvl(cust_ext.trade_sector_code, '') as trade_sector_code,
    nvl(cust_ext.trade_sector_desc, '') as trade_sector_desc,
    cust_ext.market_code_seq as market_seq,
    cust_ext.sub_market_code_seq as sub_market_seq,
    lpad(cust_ext.trade_class_seq, 4, 0) as trade_class_seq,
    lpad(cust_ext.trade_group_seq, 4, 0) as trade_group_seq,
    lpad(cust_ext.trade_type_seq, 3, 0) as trade_type_seq,
    lpad(cust_ext.trade_sector_seq, 3, 0) as trade_sector_seq,
    cust_ext.price_group,
    cust_ext.total_so_qty_discount,
    cust_ext.additional_discount,
    cust_ext.customer_rebate_group,
    cust_ext.fin_dim_customer as customer,
    fin_dim.description as customer_desc,

    nvl(cust_m.company_code, '') as company_code,
    nvl(cust_m.company_name, '') as company_name,
    nvl(cust_m.customer_name, '') as customer_name,
    nvl(cust_m.customer_type, '') as customer_type,
    nvl(cust_m.currency_code, '') as currency_code,
    nvl(cust_m.unified_customer, '') as unified_customer,
    nvl(cust_m.customer_group, '') as customer_group,
    nvl(cust_m.customer_group_name, '') as customer_group_name,
    nvl(cust_m.csr_name, '') as csr_name,
    nvl(cust_a.long_address_number, '') as long_address_number,
    nvl(cust_a.source_name, '') as source_name,
    nvl(cust_a.tax_number, '') as tax_number,
    nvl(cust_a.address_line_1, '') as address_line_1,
    nvl(cust_a.postal_code, '') as postal_code,
    nvl(cust_a.city, '') as city,
    nvl(cust_a.county, '') as county,
    nvl(cust_a.state_province, '') as state_province,
    nvl(cust_a.country_code, '') as country_code,
    nvl(cust_a.country, '') as country,

    case when cust_m.company_code in {{env_var("DBT_D365_COMPANY_FILTER")}} then 'D365' when cust_m.company_code is null then 'Legacy' else 'AX' end as source,
    case
        when cust_m.source_system_address_number is null
        then 'Obsolete'
        when left(desc_7, 2) = '##'
        then 'Obsolete'
        when hold.blocked = 1
        then 'Obsolete'
        else 'LIVE'
    end as obsolete
from ref_hierarchy_xref cust_h
inner join
    adr_wtx_cust_master_ext cust_ext
    on cust_h.source_system = cust_ext.source_system
    and cust_h.leaf_node = cust_ext.source_system_address_number
    and cust_h.company_code=cust_ext.company_code
left join
    adr_customer_master_dim cust_m on
    cust_m.customer_address_number_guid = cust_ext.customer_address_number_guid
left join
    adr_address_master_dim cust_a on
    cust_a.address_guid = cust_ext.customer_address_number_guid
-- LEFT JOIN WEETABIX.MDS_tbldimbranchchg MDS_CM
-- ON CUST_H.LEAF_NODE = MDS_CM.CUSTOMERACCOUNT
left join
    (select dataareaid, accountnum, blocked from custtable) hold
    on cust_h.node_7 = hold.accountnum
    and cust_ext.source_system_address_number = hold.accountnum
    and trim(upper(cust_ext.company_code)) = trim(upper(hold.dataareaid))
left join
    (
        select dft.value, dft.description, da.name
        from dimensionfinancialtag dft
        inner join
            dimensionattributedircategory dad
            on dft.partition = dad.partition
            and dft.financialtagcategory = dad.dircategory
        inner join
            dimensionattribute da
            on dad.partition = da.partition
            and dad.dimensionattribute = da.recid
        where da.name = 'Customer'
    ) fin_dim
    on cust_ext.fin_dim_customer = fin_dim.value

where
    cust_h.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    and cust_h.hier_name = 'CUSTOMER-SALES'
    and trim(upper(cust_ext.company_code)) in {{env_var("DBT_COMPANY_FILTER")}}
