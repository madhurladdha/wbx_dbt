{{ config(tags=["sales", "steal_summary"]) }}

with
    exc_fact_stealsku as (select * from {{ ref("src_exc_fact_stealsku") }}),
    exc_dim_pc_customer as (select * from {{ ref("src_exc_dim_pc_customer") }}),
    adr_wtx_cust_master_ext as (select * from {{ ref("dim_wbx_customer_ext") }}),
    v_wtx_cust_planning as 
    (
     select * from {{ref("dim_wbx_cust_planning")}} where COMPANY_CODE='WBX'
    ),
    exc_dim_pc_product as (select * from {{ ref("src_exc_dim_pc_product") }}),
    itm_wtx_item_master_ext as (select * from {{ ref("dim_wbx_item_ext") }})

select
    trim(cust.code) as plan_customer,
    promo_prod.code as promo_item_code,
    std_prod.code as item_code,
    fact.cannibpc,
    cu.market,
    cu.sub_market as submarket,
    cu.trade_class,
    cu.trade_sector_desc as trade_sector,
    cu.trade_type,
    wi.branding_desc as branding,
    wi.manufacturing_group_desc as manufacturing_group,
    wi.product_class_desc as product_class,
    wi.sub_product_desc as sub_product,
    wi.description as item_description,
    wi.pack_size_desc as pack_size,
    wi.category_desc as category,
    wi.promo_type_desc as promo_type,
    wi.consumer_units_in_trade_units as consumer_units,
    wi.pallet_qty,
    pwi.branding_desc as promo_branding,
    wi.manufacturing_group_desc as promo_manufacturing_group,
    pwi.product_class_desc as promo_product_class,
    pwi.sub_product_desc as promo_sub_product,
    pwi.description as promo_item_description,
    pwi.pack_size_desc as promo_pack_size,
    pwi.category_desc as promo_category,
    pwi.promo_type_desc as promo_promo_type,
    pwi.consumer_units_in_trade_units as promo_consumer_units,
    pwi.pallet_qty as promo_pallet_qty
from exc_fact_stealsku fact
left join exc_dim_pc_customer cust on fact.cust_idx = cust.idx
left join v_wtx_cust_planning cu on cu.trade_type_code = trim(cust.code)
left join exc_dim_pc_product promo_prod on fact.promo_sku_idx = promo_prod.idx
left join exc_dim_pc_product std_prod on fact.std_sku_idx = std_prod.idx
left join
    (
        select
            source_system,
            source_item_identifier,
            max(item_type) as item_type,
            max(branding_desc) as branding_desc,
            max(product_class_desc) as product_class_desc,
            max(sub_product_desc) as sub_product_desc,
            max(strategic_desc) as strategic_desc,
            max(power_brand_desc) as power_brand_desc,
            max(manufacturing_group_desc) as manufacturing_group_desc,
            max(category_desc) as category_desc,
            max(pack_size_desc) as pack_size_desc,
            max(sub_category_desc) as sub_category_desc,
            max(consumer_units_in_trade_units) as consumer_units_in_trade_units,
            max(promo_type_desc) as promo_type_desc,
            max(pallet_qty) as pallet_qty,
            max(description) as description
        from itm_wtx_item_master_ext
        group by source_system, source_item_identifier
    ) wi
    on wi.source_item_identifier = std_prod.code
left join
    (
        select
            source_system,
            source_item_identifier,
            max(item_type) as item_type,
            max(branding_desc) as branding_desc,
            max(product_class_desc) as product_class_desc,
            max(sub_product_desc) as sub_product_desc,
            max(strategic_desc) as strategic_desc,
            max(power_brand_desc) as power_brand_desc,
            max(manufacturing_group_desc) as manufacturing_group_desc,
            max(category_desc) as category_desc,
            max(pack_size_desc) as pack_size_desc,
            max(sub_category_desc) as sub_category_desc,
            max(consumer_units_in_trade_units) as consumer_units_in_trade_units,
            max(promo_type_desc) as promo_type_desc,
            max(pallet_qty) as pallet_qty,
            max(description) as description
        from itm_wtx_item_master_ext
        group by source_system, source_item_identifier
    ) pwi
    on pwi.source_item_identifier = promo_prod.code
