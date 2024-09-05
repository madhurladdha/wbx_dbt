{{
    config(
        tags=["wbx", "manufacturing", "bom", "work order"],
        materialized=env_var("DBT_MAT_TABLE"),
        snowflake_warehouse=env_var('DBT_WBX_SF_WH')
    )
}}

with
    stg_d_wbx_mfg_wo_item_bom_src as (
        select * from {{ ref("stg_d_wbx_mfg_wo_item_bom_src") }}
    ),

    bomversion as (select * from {{ ref("src_bomversion") }} where approved = 1),/*shifted this filter from below to restrict the volume at intial stage itself */
    inventtable as (select * from {{ ref("src_inventtable") }}),
    inventtablemodule as (select * from {{ ref("src_inventtablemodule") }}),
    bom as (
    select
        bomid,
        linenum,
        bomqty,
        bomqtyserie,
        scrapvar,
        unitid,
        dataareaid,
        partition,
        itemid,
        inventdimid
    from {{ ref("src_bom") }}
         ),

    inventdim as (select * from {{ ref("src_inventdim") }}),
    ecoresproduct as (select * from {{ ref("src_ecoresproduct") }}),
    ecoresproducttranslation as (
        select * from {{ ref("src_ecoresproducttranslation") }}
    ),
    inventmodelgroupitem as (select * from {{ ref("src_inventmodelgroupitem") }}),
    eco_join as (select * from {{ref('stg_ecoresproduct')}}),

    expbom(
        root_company_code,
        source_bom_identifier,
        source_bom_name,
        bom_indentifier_path,
        comp_bom_identifier,
        comp_bom_identifier_linenum,
        root_src_identiftier,
        productinventdimid,
        root_src_variant_code,
        root_business_unit_code,
        parent_src_item_identifier,
        parent_src_variant_code,
        comp_src_item_identifier,
        materialinventdimid,
        comp_src_variant_code,
        comp_required_qty,
        comp_perfection_qty,  -- *** Added for perfection qty ***
        root_qty,
        perseries,
        comp_item_uom,
        root_item_uom,
        parent_item_uom,
        comp_scrap_percent,
        parent_scrap_percent,
        effective_date,
        expiration_date,
        -- ,ROOT_ACTIVE_FLAG --*****
        active_flag,
        bom_level,
        bom_path,
        base_item_std_cost,
        pcomp_item_std_cost,
        base_item_last_cost,
        pcomp_item_last_cost,
        bom_level_active,  -- *****
        bom_level_active_flag  -- *****
    ) as (

        select *
        from stg_d_wbx_mfg_wo_item_bom_src
        union all
        select
            bv.dataareaid,
            bv.bomid as source_bom_identifier,
            bv.name,
            cast(
                b.bomid
                || '-'
                || cast(rtrim(rtrim(b.linenum, 0), '.') as varchar2(4000))
                || '/'
                || eb.bom_indentifier_path as varchar2(4000)
            ) as bom_indentifier_path,
            eb.comp_bom_identifier,
            eb.comp_bom_identifier_linenum,
            bv.itemid,
            dimproduct.inventdimid,
            dimproduct.inventsizeid,
            bv.wbxwrkctrgroupid,
            eb.parent_src_item_identifier,
            eb.parent_src_variant_code,
            eb.comp_src_item_identifier,
            dimmaterial.inventdimid,
            dimmaterial.inventsizeid,
            cast(b.bomqty as float)
            * cast((cast(100 as float) + cast(b.scrapvar as float)) as float)
            / cast(100 as float)
            / cast(b.bomqtyserie as float)
            * cast(eb.comp_required_qty as float),
            cast(b.bomqty as float)
            / cast(b.bomqtyserie as float)
            * cast(eb.comp_perfection_qty as float),  -- *** Added for perfection qty ***		
            bv.pmfbatchsize as root_qty,
            eb.perseries,
            eb.comp_item_uom,
            itmbv.unitid as root_item_uom,
            b.unitid,
            eb.comp_scrap_percent,
            b.scrapvar,
            bv.fromdate,
            bv.todate,
            -- ,bv.ACTIVE --*****
            eb.active_flag,
            eb.bom_level + 1,
            cast(bv.itemid || '|' || eb.bom_path as varchar2(4000)) as bompath,
            eb.base_item_std_cost,
            eb.pcomp_item_std_cost,
            eb.base_item_last_cost,
            eb.pcomp_item_last_cost,
            eb.bom_level_active + bv.active,  -- *****
            eb.bom_level_active_flag + eb.active_flag  -- *****
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
            inventdim p
            on p.inventdimid = b.inventdimid
            and p.dataareaid = b.dataareaid
            and p.partition = b.partition
        inner join
            expbom eb
            on eb.root_company_code = b.dataareaid
            and eb.root_src_identiftier = b.itemid
            and eb.root_src_variant_code = p.inventsizeid
        inner join
            inventdim dimproduct
            on dimproduct.inventdimid = bv.inventdimid
            and dimproduct.dataareaid = bv.dataareaid
            and dimproduct.partition = bv.partition
        inner join
            inventdim dimmaterial
            on dimmaterial.inventdimid = eb.materialinventdimid
            and dimmaterial.dataareaid = eb.root_company_code
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
            and erptmaterial.partition = bv.partition*/
        where
            -- bv.approved = 1 /* 15/07/2024 appying this filter above at source level cte*/
            -- AND bv.active = 1 --*****
             eb.active_flag = 1
            and eb.bom_level + 1 = eb.bom_level_active_flag + eb.active_flag  -- *****
            and (eb.bom_level_active_flag + eb.active_flag)
            - (eb.bom_level_active + bv.active)
            < 2  -- allows for multiple formulas for same variant, and lets only one pass *****
            and (
                (eb.bom_level + 1 = eb.bom_level_active + bv.active and bv.active = 1)
                or bv.active = 0
            )  -- *****
          
            and eb.bom_level + 1 < 10  -- Prevent infinite BOMs
    ),
    expbom1 as (
        select
            source_bom_identifier,
            source_bom_name,
            cast(expbom.bom_indentifier_path as varchar2(4000)) as bom_indentifier_path,
            comp_bom_identifier,
            rtrim(
                rtrim(comp_bom_identifier_linenum, 0), '.'
            ) as comp_bom_identifier_linenum,
            expbom.root_company_code,
            expbom.root_src_identiftier,
            root_src_variant_code,
            root_business_unit_code,
            parent_src_item_identifier,
            parent_src_variant_code,
            comp_src_item_identifier,
            comp_src_variant_code,
            comp_required_qty,
            comp_perfection_qty,
            root_qty,
            expbom.comp_scrap_percent,
            expbom.parent_scrap_percent,
            comp_item_uom,
            root_item_uom,
            parent_item_uom,
            effective_date,
            case
                expiration_date when '1900-01-01' then '2050-12-31' else expiration_date
            end as expiration_date,
            active_flag,
            bom_level,
            bom_path,
            base_item_std_cost,
            pcomp_item_std_cost,
            base_item_last_cost,
            pcomp_item_last_cost,
            to_date(convert_timezone('UTC', current_timestamp)) as source_updated_date
        -- ,UPPER(IMGI.MODELGROUPID) AS ITEM_MODEL_GROUP
        from expbom
        -- INNER JOIN INVENTMODELGROUPITEM IMGI ON ExpBom.COMP_SRC_ITEM_IDENTIFIER =
        -- IMGI.ITEMID AND ExpBom.ROOT_COMPANY_CODE = IMGI.ITEMDATAAREAID
        where
            (
                (root_business_unit_code <> 'Co-pack')
                or (root_business_unit_code = 'Co-pack' and bom_level = 1)
            )
    )

select expbom1.*, upper(imgi.modelgroupid) as item_model_group
from expbom1
inner join
    inventmodelgroupitem imgi
    on expbom1.comp_src_item_identifier = imgi.itemid
    and expbom1.root_company_code = imgi.itemdataareaid
