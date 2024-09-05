{{ config(tags=["wbx", "manufacturing", "bom", "work order"],
  materialized=env_var('DBT_MAT_TABLE'),
          ) }}


with
    bomversion as (select * from {{ ref("src_bomversion") }}),
    inventtable as (select * from {{ ref("src_inventtable") }}),
    inventtablemodule as (select * from {{ ref("src_inventtablemodule") }}),
    bom as (select * from {{ ref("src_bom") }}),
    inventdim as (select * from {{ ref("src_inventdim") }}),
    ecoresproduct as (select * from {{ ref("src_ecoresproduct") }}),
    ecoresproducttranslation as (
        select * from {{ ref("src_ecoresproducttranslation") }}
    ),
    eco_join as (select * from {{ref('stg_ecoresproduct')}}),

    final as (
        select
            bv.dataareaid as root_company_code,
            bv.bomid as source_bom_identifier,
            bv.name as source_bom_name,
            cast(
                bv.bomid
                || '-'
                || cast(rtrim(rtrim(b.linenum, 0), '.') as varchar2(4000)) as varchar2(
                    4000
                )
            ) as bom_identifier_path,
            bv.bomid as comp_bom_identifier,
            b.linenum as comp_bom_identifier_linenum,
            bv.itemid as root_src_identiftier,
            dimproduct.inventdimid as productinventdimid,
            dimproduct.inventsizeid as root_src_variant_code,
            dimmaterial.inventlocationid as root_business_unit_code,
            bv.itemid as parent_src_item_identifier,
            dimproduct.inventsizeid as parent_src_variant_code,
            b.itemid as comp_src_item_identifier,
            dimmaterial.inventdimid as materialinventdimid,
            dimmaterial.inventsizeid as comp_src_variant_code,
            cast(b.bomqty as float)
            * cast((cast(100 as float) + cast(b.scrapvar as float)) as float)
            / cast(100 as float) as comp_required_qty,  -- AS ExpMarterialQtyVAR
            cast(b.bomqty as float) as comp_perfection_qty,  -- *** Added for perfection qty ***
            bv.pmfbatchsize as root_qty,
            b.bomqtyserie as perseries,
            b.unitid as comp_item_uom,
            itmbv.unitid as root_item_uom,
            itmbv.unitid as parent_item_uom,
            b.scrapvar as comp_scrap_percent,
            cast(0 as number(38, 10)) as parent_scrap_percent,
            bv.fromdate as effective_date,
            bv.todate as expiration_date,
            -- ,bv.ACTIVE  AS
            -- ROOT_ACTIVE_FLAG --*****
            bv.active as active_flag,
            1 as bom_level,
            cast(bv.itemid || '|' || b.itemid as varchar2(4000)) as bom_path,
            0 as base_item_std_cost,
            0 as pcomp_item_std_cost,
            0 as base_item_last_cost,
            0 as pcomp_item_last_cost,
            1 as bom_level_active,  -- *****
            1 as bom_level_active_flag  -- *****
        from bomversion bv
        inner join
            inventtable ibv
            on ibv.dataareaid = bv.dataareaid
            and ibv.itemid = bv.itemid
            and ibv.partition = bv.partition
        inner join
            inventtablemodule itmbv
            on itmbv.dataareaid = bv.dataareaid
            and itmbv.itemid = bv.itemid
            and itmbv.partition = bv.partition
            and itmbv.moduletype = 0
        inner join
            bom b
            on b.dataareaid = bv.dataareaid
            and b.bomid = bv.bomid
            and b.partition = bv.partition
        inner join
            inventtable ib
            on ib.dataareaid = b.dataareaid
            and ib.itemid = b.itemid
            and ib.partition = b.partition
        inner join
            inventdim dimproduct
            on dimproduct.inventdimid = bv.inventdimid
            and dimproduct.dataareaid = bv.dataareaid
            and dimproduct.partition = bv.partition
        inner join
            inventdim dimmaterial
            on dimmaterial.inventdimid = b.inventdimid
            and dimmaterial.dataareaid = b.dataareaid
            and dimmaterial.partition = b.partition
        inner join
            ecoresproduct erpproduct
            on ibv.product = erpproduct.recid
            and erpproduct.partition = bv.partition
        inner join
            ecoresproducttranslation erptproduct
            on erptproduct.product = erpproduct.recid
            and erptproduct.partition = bv.partition
        inner join eco_join
             on ib.product = erp_recid  and erp_partition = bv.partition and erpt_partition = bv.partition
        /*inner join
            ecoresproduct erpmaterial
            on ib.product = erpmaterial.recid
            and erpmaterial.partition = bv.partition
        inner join
            ecoresproducttranslation erptmaterial
            on erptmaterial.product = erpmaterial.recid
            and erptmaterial.partition = bv.partition */
        where bv.approved = 1
    )

select *
from final
