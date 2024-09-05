{{ config(tags=["wbx", "manufacturing", "yield", "stock_adj", "inventory"]) }}

with
    dalvv as (select * from {{ ref("src_dimensionattributelevelvalueview") }}),
    da as (select * from {{ ref("src_dimensionattribute") }}),
    ijt as (select * from {{ ref("src_inventjournaltrans") }}),
    itab as (select * from {{ ref("src_inventjournaltable") }}),
    itm as (select * from {{ ref("src_inventtablemodule") }}),
    r as (select * from {{ ref("src_reasontableref") }}),
    i as (select * from {{ ref("src_inventdim") }}),
    ito as (select * from {{ ref("src_inventtransorigin") }}),
    inventtransposting as (select * from {{ ref("src_inventtransposting") }}),
    itp as (
        select distinct
            inventtransorigin,
            voucher,
            dataareaid,
            partition,
            offsetledgerdimension,
            isposted
        from inventtransposting
        where isposted = 1
    ),
    d as (select * from {{ ref("src_dimensionattributevaluecombo") }}),
    dm1 as (
        select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, da.partition
        from dalvv
        inner join da on dalvv.dimensionattribute = da.recid
        where da.name = 'Sites'
    ),
    dm4 as (
        select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, da.partition
        from dalvv
        inner join da on dalvv.dimensionattribute = da.recid
        where da.name = 'Plant'
    ),
    dm6 as (
        select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, da.partition
        from dalvv
        inner join da on dalvv.dimensionattribute = da.recid
        where da.name = 'ProductClass'
    ),
    dm7 as (
        select dalvv.displayvalue, dalvv.valuecombinationrecid, da.name, da.partition
        from dalvv
        inner join da on dalvv.dimensionattribute = da.recid
        where da.name = 'MainAccount'
    ),
    final as (
        select
            UPPER('{{env_var("DBT_SOURCE_SYSTEM")}}') as source_system,
            dm7.displayvalue as source_account_identifier,
            ijt.recid as source_record_id,
            UPPER(ijt.dataareaid) as company_code,
            itab.journalnameid as related_document_type,
            itab.description as related_document_desc,
            ijt.journalid as related_document_number,
            nvl(dm1.displayvalue, '') as site,
            nvl(dm6.displayvalue, '') as product_class,
            ijt.itemid as source_item_identifier,
            UPPER(nvl(dm4.displayvalue, '')) as source_business_unit_code,
            i.inventsizeid as variant_code,
            ijt.transdate as transaction_date,
            ijt.modifieddatetime as gl_date,
            ijt.qty as transaction_qty,
            ijt.costamount as transaction_amt,
            itm.unitid as transaction_uom,
            'GBP' as transaction_currency,
            ijt.voucher,
            nvl(r.reasoncomment, '') as remark_txt,
            i.inventsiteid as stock_site,
            current_date as load_date,
            current_date as update_date
        from ijt
        inner join
            itab on itab.journalid = ijt.journalid and ijt.dataareaid = itab.dataareaid
        inner join
            itm
            on ijt.partition = itm.partition
            and ijt.dataareaid = itm.dataareaid
            and ijt.itemid = itm.itemid
            and 0 = itm.moduletype
        left join r on ijt.reasonrefrecid = r.recid and ijt.dataareaid = r.dataareaid
        join i on i.inventdimid = ijt.inventdimid and ijt.dataareaid = i.dataareaid
        join ito on ito.inventtransid = ijt.inventtransid
        join
            itp
            on itp.voucher = ijt.voucher
            and isposted = 1
            and itp.inventtransorigin = ito.recid
        join d on d.recid = itp.offsetledgerdimension and d.displayvalue like '521%'
        inner join
            dm7
            on itp.offsetledgerdimension = dm7.valuecombinationrecid
            and itp.partition = dm7.partition
        left outer join
            dm1
            on itp.offsetledgerdimension = dm1.valuecombinationrecid
            and itp.partition = dm1.partition
        left outer join
            dm6
            on itp.offsetledgerdimension = dm6.valuecombinationrecid
            and itp.partition = dm6.partition
        left outer join
            dm4
            on itp.offsetledgerdimension = dm4.valuecombinationrecid
            and itp.partition = dm4.partition
    )

select *
from final
