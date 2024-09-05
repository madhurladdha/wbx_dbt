with
    forecastsales as (select * from {{ ref("src_forecastsales") }}),
    inventdim as (select * from {{ ref("src_inventdim") }}),
    inventtable as (select * from {{ ref("src_inventtable") }}),
    ecoresproduct as (select * from {{ ref("src_ecoresproduct") }}),
    ecoresproductvariantdimvalue as (
        select * from {{ ref("src_ecoresproductvariantdimvalue") }}
    ),
    ecoressize as (select * from {{ ref("src_ecoressize") }}),
    ecoresproducttranslation as (
        select * from {{ ref("src_ecoresproducttranslation") }}
    ),
    ecoresproducttranslation as (
        select * from {{ ref("src_ecoresproducttranslation") }}
    ),
    inventdimcombination as (select * from {{ ref("src_inventdimcombination") }}),
    forecastitemallocationline as (
        select * from {{ ref("src_forecastitemallocationline") }}
    ),

    forcastandinv as (
        select
            row_number() over (order by fs.dataareaid, itemid, startdate) as tablerow,
            row_number() over (
                partition by fs.dataareaid, itemid, id.inventsizeid
                order by fs.dataareaid, itemid, startdate
            ) as partitionrow,
            fs.dataareaid,
            itemid,
            id.inventsizeid as variant_code,
            startdate
        from forecastsales fs
        inner join
            inventdim id
            on fs.dataareaid = id.dataareaid
            and fs.inventdimid = id.inventdimid
        where modelid like '%_VAR' and (to_date(startdate) + 6) > to_date(current_date)
    ),
    fsgroup as (
        select
            tablerow,
            partitionrow,
            dataareaid,
            itemid,
            variant_code,
            startdate,
            tablerow - partitionrow as groupedrow
        from forcastandinv
    ),
    fs as (
        select
            dataareaid,
            itemid,
            variant_code,
            min(startdate) as effective_date,
            max(to_date(startdate)) + 6 as expiration_date
        from fsgroup
        group by dataareaid, itemid, variant_code, groupedrow
    ),
    source as (
        select
            it.itemid as source_item_identifier,
            it.dataareaid as company_code,
            cast(to_char(s.name) as varchar2(255)) as variant_code,
            eptv.name as variant_desc,
            nvl(erpproduct.notinuse, 0) as variant_status,
            nvl(fial.allocationid, ' ') as item_allocation_key,
            nvl(
                to_date(fs.effective_date), to_date('1900/01/01', 'YYYY/MM/DD')
            ) as effective_date,
            nvl(
                fs.expiration_date, to_date('1900/01/01', 'YYYY/MM/DD')
            ) as expiration_date,
            case
                when
                    nvl(fs.effective_date, to_date('2050-12-31'))
                    <= to_date(current_date)
                then 1
                else 0
            end as active_flag,
            to_date(current_date) as source_update_date
        from inventtable it
        inner join ecoresproduct erpproduct on it.product = erpproduct.recid
        inner join
            (
                select distinct nvl(productmaster, recid) as productmaster, recid
                from ecoresproduct
            ) pm
            on it.product = pm.productmaster
        inner join
            ecoresproductvariantdimvalue pv on pm.recid = pv.distinctproductvariant
        inner join ecoressize s on pv.size_ = s.recid
        inner join ecoresproducttranslation eptv on pm.recid = eptv.product
        inner join
            inventdimcombination idc
            on pv.distinctproductvariant = idc.distinctproductvariant
        inner join
            inventdim id
            on idc.inventdimid = id.inventdimid
            and idc.dataareaid = id.dataareaid
        left outer join
            forecastitemallocationline fial
            on it.itemid = fial.itemid
            and idc.inventdimid = fial.inventdimid
            and it.dataareaid = fial.dataareaid
        left outer join
            fs
            on it.itemid = fs.itemid
            and it.dataareaid = fs.dataareaid
            and s.name = fs.variant_code
        union
        select
            it.itemid as source_item_identifier,
            it.dataareaid as company_code,
            to_char(' ') as variant_code,
            erptproduct.name as variant_desc,
            0 as variant_status,
            nvl(fial.allocationid, ' ') as item_allocation_key,
            to_date('1900/01/01', 'YYYY/MM/DD') as effective_date,
            to_date('1900/01/01', 'YYYY/MM/DD') as expiration_date,
            0 as active_flag,
            to_date(current_date) as source_update_date
        from inventtable it
        inner join ecoresproduct erpproduct on it.product = erpproduct.recid
        inner join
            ecoresproducttranslation erptproduct
            on erptproduct.product = erpproduct.recid
        left outer join
            forecastitemallocationline fial
            on it.itemid = fial.itemid
            and it.dataareaid = fial.dataareaid
        where
            (
                (it.itemid like 'TR%' and length(it.itemid) = 8)
                or (it.itemid like 'R%' and length(it.itemid) = 7)
            )
    )

select *
from source
