{{ config(tags=["product_hierarchy"]) }}

with
    ref_hierarchy_xref as (select * from {{ ref("xref_wbx_hierarchy") }}),
    itm_wtx_item_master_ext as (select * from {{ ref("dim_wbx_item_ext") }}),
    itm_item_master_dim as (select * from {{ ref("dim_wbx_item") }}),
    dimensionattributevaluesetitem as (
        select * from {{ ref("src_dimensionattributevaluesetitem") }}
    ),
    dimensionattributevalue as (select * from {{ ref("src_dimensionattributevalue") }}),
    dimensionattribute as (select * from {{ ref("src_dimensionattribute") }}),
    inventitembarcode as (select * from {{ ref("src_inventitembarcode") }}),
    inventdim as (select * from {{ ref("src_inventdim") }}),
    inventtable as (select * from {{ ref("src_inventtable") }}),
    whsinventtable as (select * from {{ ref("src_whsinventtable") }}),
    wbxinventtableext as (select * from {{ ref("src_wbxinventtableext") }}),
    inventitemgroupitem as (select * from {{ ref("src_inventitemgroupitem") }}),
    inventtablemodule as (select * from {{ ref("src_inventtablemodule") }}),
    ecoresproduct as (select * from {{ ref("src_ecoresproduct") }}),
    ecoresproducttranslation as (
        select * from {{ ref("src_ecoresproducttranslation") }}
    ),
    prd as (
        select
            davs.partition, davs.dimensionattributevalueset, davs.displayvalue, da.name
        from dimensionattributevaluesetitem davs
        -- (DEFAULTDIMENSION field's value above)
        inner join
            dimensionattributevalue dav
            on davs.partition = dav.partition
            and davs.dimensionattributevalue = dav.recid
        inner join
            dimensionattribute da
            on dav.partition = da.partition
            and dav.dimensionattribute = da.recid
        where da.name in ('Product')
    ),

    -- Find highest variant
    fg_var as (
        select
            iibc.partition,
            iibc.itemid,
            iibc.barcodesetupid,
            max(id.inventsizeid) as variant
        from inventitembarcode iibc
        inner join
            inventdim id
            on iibc.partition = id.partition
            and iibc.dataareaid = id.dataareaid
            and iibc.inventdimid = id.inventdimid
        where
            is_real(try_to_numeric(iibc.itemid))
            and len(iibc.itemid) = 5
            and UPPER(iibc.dataareaid) in {{env_var("DBT_COMPANY_FILTER")}}
            and is_real(try_to_numeric(inventsizeid))
        group by iibc.partition, iibc.itemid, iibc.barcodesetupid
    ),

    tuc as (
        select i.partition, i.itemid, i.itembarcode
        from inventitembarcode i
        inner join
            inventdim id
            on i.partition = id.partition
            and i.dataareaid = id.dataareaid
            and i.inventdimid = id.inventdimid
        inner join
            fg_var
            on i.partition = fg_var.partition
            and i.itemid = fg_var.itemid
            and id.inventsizeid = fg_var.variant
            and i.barcodesetupid = 'TUC'
    ),

    dun as (
        select i.partition, i.itemid, i.itembarcode
        from inventitembarcode i
        inner join
            inventdim id
            on i.partition = id.partition
            and i.dataareaid = id.dataareaid
            and i.inventdimid = id.inventdimid
        inner join
            fg_var
            on i.partition = fg_var.partition
            and i.itemid = fg_var.itemid
            and id.inventsizeid = fg_var.variant
            and i.barcodesetupid = 'DUN'
    ),

    ean as (
        select i.partition, i.itemid, i.itembarcode
        from inventitembarcode i
        inner join
            inventdim id
            on i.partition = id.partition
            and i.dataareaid = id.dataareaid
            and i.inventdimid = id.inventdimid
        inner join
            fg_var
            on i.partition = fg_var.partition
            and i.itemid = fg_var.itemid
            and id.inventsizeid = fg_var.variant
            and i.barcodesetupid = 'EAN13'
    ),
    v_wtx_product_barcode as (
        select distinct
            it.itemid,
            erpt.name as description,
          --  wite.consumerunits as packspercase,
            it.netweight as netweight,
            it.taraweight as tareweight,
            it.netweight + it.taraweight as grossweight,
            it.grossdepth as grossdepth,
            it.grosswidth as grosswidth,
            it.grossheight as grossheight,
            it.unitvolume as volume,
            it.pdsitemrebategroupid as itemrebategroup,
            it.pdsshelflife as shelflifedays,
          --  wite.pallettype,
         --   wite.palletqty as palletqty,
          --  wite.palletqtyperlayer as casesperlayer,
            iigi.itemgroupid,
            nvl(prd.displayvalue, '') as productdimension,
            it.inventstrategiccodeid as strategic,
            it.inventbrandingcodeid as branding,
            it.inventproductclasscodeid as productclass,
            it.inventsubproductcodeid as subproduct,
            it.inventpacksizecodeid as packsize,
            it.avpweight as avp_weight,
            it.consumerunit as consumerunits,
            it.avpflag as avp_flag,
            it.pmpflag as pmp_flag,
            it.currentflag as currentflag,
            nvl(tuc.itembarcode, '') as tuc,
            nvl(dun.itembarcode, '') as dun,
            nvl(ean.itembarcode, '') as ean13,
            it.dataareaid
        from inventtable it
        left outer join
            whsinventtable wit
            on it.itemid = wit.itemid
            and it.dataareaid = wit.dataareaid
            and it.partition = wit.partition
      /*  inner join
            wbxinventtableext wite
            on it.itemid = wite.itemid
            and it.dataareaid = wite.dataareaid
            and it.partition = wite.partition */
        inner join
            inventitemgroupitem iigi
            on it.itemid = iigi.itemid
            and it.dataareaid = iigi.itemdataareaid
            and it.partition = iigi.partition
        inner join
            inventtablemodule itm
            on it.dataareaid = itm.dataareaid
            and it.partition = itm.partition
            and it.itemid = itm.itemid
            and itm.moduletype = 0
        inner join
            ecoresproduct erp on it.product = erp.recid and it.partition = erp.partition
        inner join
            ecoresproducttranslation erpt
            on erp.recid = erpt.product
            and erp.partition = erpt.partition
        left outer join
            prd
            on it.partition = prd.partition
            and it.defaultdimension = prd.dimensionattributevalueset
        left outer join tuc on it.partition = tuc.partition and it.itemid = tuc.itemid
        left outer join dun on it.partition = dun.partition and it.itemid = dun.itemid
        left outer join ean on it.partition = ean.partition and it.itemid = ean.itemid
        where
            is_real(try_to_numeric(it.itemid))
            and len(it.itemid) = 5
            and UPPER(it.dataareaid) in {{env_var("DBT_COMPANY_FILTER")}}
            and it.itemid not in ('09487')
        order by 1
    ),
    dimensionfinancialtag as (select * from {{ ref("src_dimensionfinancialtag") }}),
    dimensionattributedircategory as (
        select * from {{ ref("src_dimensionattributedircategory") }}
    ),
    whsfilters as (select * from {{ ref("src_whsfilters") }}),

final as (
select
    to_number(prod_h.node_level, 38, 0) as node_level,
    node_1 as branding_code,
    desc_1 as branding,
    node_2 as product_code,
    desc_2 as product_class,
    node_3 as sub_product_code,
    desc_3 as sub_product,
    node_4 as item_sku,
    desc_4 as item_desc,
    UPPER(bar.dataareaid) as  company,
    nvl(prod_e.strategic_code, '') as strategic_code,
    nvl(prod_e.strategic_desc, '') as strategic_desc,
    nvl(prod_e.power_brand_code, '') as power_brand_code,
    nvl(prod_e.power_brand_desc, '') as power_brand_desc,
    nvl(prod_e.manufacturing_group_code, '') as manufacturing_group_code,
    nvl(prod_e.manufacturing_group_desc, '') as manufacturing_group_desc,
    nvl(prod_e.pack_size_code, '') as pack_size_code,
    nvl(prod_e.pack_size_desc, '') as pack_size_desc,
    nvl(prod_e.category_code, '') as category_code,
    nvl(prod_e.category_desc, '') as category_desc,
    nvl(prod_e.promo_type_code, '') as promo_type_code,
    nvl(prod_e.promo_type_desc, '') as promo_type_desc,
    nvl(prod_e.sub_category_code, '') as sub_category_code,
    nvl(prod_e.sub_category_desc, '') as sub_category_desc,
    /*prod_e.branding_seq  0 as branding_seq ,
    lpad(prod_e.product_class_seq, 4, 0) as product_class_seq,
    lpad(prod_e.sub_product_seq, 4, 0) as sub_product_seq,
    lpad(prod_e.strategic_seq, 4, 0) as strategic_seq,
    lpad(prod_e.power_brand_seq, 3, 0) as power_brand_seq,
    lpad(prod_e.manufacturing_group_seq, 3, 0) as manufacturing_group_seq,
    lpad(prod_e.pack_size_seq, 5, 0) as pack_size_seq,
    prod_e.category_seq,
    lpad(prod_e.promo_type_seq, 2, 0) as promo_type_seq,
    prod_e.sub_category_seq, 
    As part of D365 all the SEQ number was made 0 as from D365 all the SEQ are hardcoded to 0.This was discussed with Dave in call
    and we agreed to make it 0 */ 
    0 as branding_seq,
    0 as product_class_seq,
    0 as sub_product_seq,
    0 as strategic_seq,
    0 as power_brand_seq,
    0 as manufacturing_group_seq,
    0 as pack_size_seq,
    0 as category_seq,
    0 as promo_type_seq,
    0 as sub_category_seq,
    prod_e.net_weight,
    prod_e.tare_weight,
    prod_e.avp_weight,
    nvl(prod_e.avp_flag, '') as avp_flag,
    case
        when prod_e.pmp_flag = 1 then 'Y' when prod_e.pmp_flag = 0 then 'N' else ''
    end as pmp_flag,
    prod_e.consumer_units_in_trade_units,
    prod_e.consumer_units,
    prod_e.pallet_qty,
    nvl(prod_e.current_flag, '') as current_flag,
    prod_e.gross_weight,
    prod_e.gross_depth,
    prod_e.gross_width,
    prod_e.gross_height,
    to_number(prod_e.pallet_qty_per_layer, 38, 0) as pallet_qty_per_layer,
    prod_e.exclude_indicator,
    prod_e.fin_dim_product,
    fin_dim.description as fin_dim_product_desc,
    prod_e.filter_code1 as filter_code1,
    whs_f1.description as filter_desc1,
    prod_e.filter_code2 as filter_code2,
    whs_f2.description as filter_desc2,
    prod_e.pallet_type as pallet_type,
    prod_e.pallet_config as pallet_config,

    nvl(itm_m.stock_type, '') as stock_type,
    nvl(itm_m.stock_desc, '') as stock_desc,
    nvl(itm_m.item_type, '') as item_type,
    nvl(itm_m.primary_uom, '') as primary_uom,
    nvl(itm_m.primary_uom_desc, '') as primary_uom_desc,
    nvl(itm_m.obsolete_flag, '') as obsolete_flag_ax,
    bar.tuc,
    bar.dun,
    bar.ean13,

    case
        when itm_m.source_item_identifier is null then 'Legacy' else 'D365'
    end as source,
    case
        when itm_m.source_item_identifier is null
        then 'Obsolete'
        when left(desc_4, 2) = '##'
        then 'Obsolete'
        else 'LIVE'
    end as obsolete

from ref_hierarchy_xref prod_h
inner join
    (
        select
            source_system,
            source_item_identifier,
            max(strategic_code) as strategic_code,
            max(strategic_desc) as strategic_desc,
            max(power_brand_code) as power_brand_code,
            max(power_brand_desc) as power_brand_desc,
            max(manufacturing_group_code) as manufacturing_group_code,
            max(manufacturing_group_desc) as manufacturing_group_desc,
            max(pack_size_code) as pack_size_code,
            max(pack_size_desc) as pack_size_desc,
            max(category_code) as category_code,
            max(category_desc) as category_desc,
            max(promo_type_code) as promo_type_code,
            max(promo_type_desc) as promo_type_desc,
            max(sub_category_code) as sub_category_code,
            max(sub_category_desc) as sub_category_desc,
            max(net_weight) as net_weight,
            max(tare_weight) as tare_weight,
            max(avp_weight) as avp_weight,
            max(avp_flag) as avp_flag,
            max(pmp_flag) as pmp_flag,
            max(consumer_units_in_trade_units) as consumer_units_in_trade_units,
            max(consumer_units) as consumer_units,
            max(pallet_qty) as pallet_qty,
            max(current_flag) as current_flag,
            max(gross_weight) as gross_weight,
            max(gross_depth) as gross_depth,
            max(gross_width) as gross_width,
            max(gross_height) as gross_height,
            max(pallet_qty_per_layer) as pallet_qty_per_layer,
            max(exclude_indicator) as exclude_indicator,
            max(branding_seq) as branding_seq,
            max(product_class_seq) as product_class_seq,
            max(sub_product_seq) as sub_product_seq,
            max(strategic_seq) as strategic_seq,
            max(power_brand_seq) as power_brand_seq,
            max(manufacturing_group_seq) as manufacturing_group_seq,
            max(pack_size_seq) as pack_size_seq,
            max(category_seq) as category_seq,
            max(promo_type_seq) as promo_type_seq,
            max(sub_category_seq) as sub_category_seq,
            max(fin_dim_product) as fin_dim_product,
            max(whs_filter_code) as filter_code1,
            max(whs_filter_code2) as filter_code2,
            max(pallet_type) as pallet_type,
            max(pallet_config) as pallet_config
        from itm_wtx_item_master_ext
        group by source_system, source_item_identifier
    ) prod_e
    on prod_h.source_system = prod_e.source_system
    and prod_h.leaf_node = prod_e.source_item_identifier

left join
    (
        select
            a.source_system,
            a.source_item_identifier,
            max(case_item_number) as case_item_number,
            max(stock_type) as stock_type,
            max(stock_desc) as stock_desc,
            max(item_type) as item_type,
            max(primary_uom) as primary_uom,
            max(primary_uom_desc) as primary_uom_desc,
            max(obsolete_flag) as obsolete_flag
        from itm_item_master_dim a
        where
            a.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
            and a.update_date = (
                select max(b.update_date)
                from itm_item_master_dim b
                where
                    a.source_system = b.source_system
                    and a.source_item_identifier = b.source_item_identifier
            )
        group by source_system, source_item_identifier
    ) itm_m
    on itm_m.source_system = prod_h.source_system
    and itm_m.source_item_identifier = prod_h.leaf_node

left join v_wtx_product_barcode bar on prod_h.node_4 = bar.itemid

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
        where da.name = 'Product'
    ) fin_dim
    on prod_e.fin_dim_product = fin_dim.value

left join
    (
        select trim(filternum) as filternum, max(description) as description
        from whsfilters
        where upper(trim(dataareaid)) in {{env_var("DBT_COMPANY_FILTER")}}
        group by trim(filternum)
    ) whs_f1
    on whs_f1.filternum = filter_code1
left join
    (
        select trim(filternum) as filternum, max(description) as description
        from whsfilters
        where upper(trim(dataareaid)) in {{env_var("DBT_COMPANY_FILTER")}}
        group by trim(filternum)
    ) whs_f2
    on whs_f2.filternum = filter_code2

where
    prod_h.source_system = '{{env_var("DBT_SOURCE_SYSTEM")}}'
    and prod_h.hier_name = 'ITEM-SALES' ) 

select * exclude(company) from final qualify row_number() over(partition by item_sku  order by company desc) = 1
