{{
    config(

    materialized = env_var('DBT_MAT_INCREMENTAL'),
    transient = false,
    tags = "rdm_core",
    unique_key = 'unique_key',
    on_schema_change='sync_all_columns'
    )
}}
WITH HIST_ITEM as (
    select * from {{ref('conv_dim_wbx_item_ext')}}

),

STG as (
 select {{ dbt_utils.surrogate_key(['ITEM_GUID','BUSINESS_UNIT_ADDRESS_GUID']) }} AS UNIQUE_KEY,* from  {{ref('stg_d_wbx_item_ext')}}
),


new_dim as (
select
 cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(a.item_guid as text(255) ) as item_guid  ,
cast(b.item_guid_old as text(255) ) as item_guid_old  ,
cast(a.BUSINESS_UNIT_ADDRESS_GUID as text(255)) as BUSINESS_UNIT_ADDRESS_GUID,
cast(substring(a.source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,
cast(substring(a.source_business_unit_code,1,60) as text(60) ) as source_business_unit_code  ,
cast(substring(a.description,1,60) as text(60) ) as description  ,
cast(substring(a.item_type,1,255) as text(255) ) as item_type  ,
cast(substring(a.branding_code,1,60) as text(60) ) as branding_code  ,
cast(substring(a.branding_desc,1,255) as text(255) ) as branding_desc  ,
cast(a.branding_seq as number(38,0) ) as branding_seq  ,
cast(substring(a.product_class_code,1,60) as text(60) ) as product_class_code  ,
cast(substring(a.product_class_desc,1,255) as text(255) ) as product_class_desc  ,
cast(a.product_class_seq as number(38,0) ) as product_class_seq  ,
cast(substring(a.sub_product_code,1,60) as text(60) ) as sub_product_code  ,
cast(substring(a.sub_product_desc,1,255) as text(255) ) as sub_product_desc  ,
cast(a.sub_product_seq as number(38,0) ) as sub_product_seq  ,
cast(substring(a.strategic_code,1,60) as text(60) ) as strategic_code  ,
cast(substring(a.strategic_desc,1,255) as text(255) ) as strategic_desc  ,
cast(a.strategic_seq as number(38,0) ) as strategic_seq  ,
cast(substring(a.power_brand_code,1,60) as text(60) ) as power_brand_code  ,
cast(substring(a.power_brand_desc,1,255) as text(255) ) as power_brand_desc  ,
cast(a.power_brand_seq as number(38,0) ) as power_brand_seq  ,
cast(substring(a.manufacturing_group_code,1,60) as text(60) ) as manufacturing_group_code  ,
cast(substring(a.manufacturing_group_desc,1,255) as text(255) ) as manufacturing_group_desc  ,
cast(a.manufacturing_group_seq as number(38,0) ) as manufacturing_group_seq  ,
cast(substring(a.pack_size_code,1,60) as text(60) ) as pack_size_code  ,
cast(substring(a.pack_size_desc,1,255) as text(255) ) as pack_size_desc  ,
cast(a.pack_size_seq as number(38,0) ) as pack_size_seq  ,
cast(substring(a.category_code,1,60) as text(60) ) as category_code  ,
cast(substring(a.category_desc,1,255) as text(255) ) as category_desc  ,
cast(a.category_seq as number(38,0) ) as category_seq  ,
cast(substring(a.promo_type_code,1,60) as text(60) ) as promo_type_code  ,
cast(substring(a.promo_type_desc,1,255) as text(255) ) as promo_type_desc  ,
cast(a.promo_type_seq as number(38,0) ) as promo_type_seq  ,
cast(substring(a.sub_category_code,1,60) as text(60) ) as sub_category_code  ,
cast(substring(a.sub_category_desc,1,255) as text(255) ) as sub_category_desc  ,
cast(a.sub_category_seq as number(38,0) ) as sub_category_seq  ,
cast(a.net_weight as number(38,10) ) as net_weight  ,
cast(a.tare_weight as number(38,10) ) as tare_weight  ,
cast(a.avp_weight as number(38,10) ) as avp_weight  ,
cast(substring(a.avp_flag,1,1) as text(1) ) as avp_flag  ,
cast(a.consumer_units_in_trade_units as number(38,0) ) as consumer_units_in_trade_units  ,
cast(a.pallet_qty as number(38,10) ) as pallet_qty  ,
cast(substring(a.current_flag,1,1) as text(1) ) as current_flag  ,
cast(a.gross_weight as number(38,10) ) as gross_weight  ,
cast(a.gross_depth as number(38,10) ) as gross_depth  ,
cast(a.gross_width as number(38,10) ) as gross_width  ,
cast(a.gross_height as number(38,10) ) as gross_height  ,
cast(substring(a.pmp_flag,1,1) as text(1) ) as pmp_flag  ,
cast(a.consumer_units as number(38,0) ) as consumer_units  ,
cast(a.pallet_qty_per_layer as number(38,10) ) as pallet_qty_per_layer  ,
cast(substring(a.item_vat_group,1,60) as text(60) ) as item_vat_group  ,
cast(substring(a.exclude_indicator,1,60) as text(60) ) as exclude_indicator  ,
cast(a.date_inserted as timestamp_ntz(9) ) as date_inserted  ,
cast(a.date_updated as timestamp_ntz(9) ) as date_updated  ,
cast(substring(a.mangrpcd_site,1,255) as text(255) ) as mangrpcd_site  ,
cast(substring(a.mangrpcd_plant,1,255) as text(255) ) as mangrpcd_plant  ,
cast(substring(a.mangrpcd_copack_flag,1,1) as text(1) ) as mangrpcd_copack_flag  ,
cast(substring(a.fin_dim_cost_centre,1,255) as text(255) ) as fin_dim_cost_centre  ,
cast(substring(a.fin_dim_product,1,255) as text(255) ) as fin_dim_product  ,
cast(substring(a.fin_dim_department,1,255) as text(255) ) as fin_dim_department  ,
cast(substring(a.fin_dim_site,1,255) as text(255) ) as fin_dim_site  ,
cast(substring(a.whs_filter_code,1,20) as text(20) ) as whs_filter_code  ,
cast(substring(a.whs_filter_code2,1,20) as text(20) ) as whs_filter_code2  ,
cast(substring(a.whs_filter_code3,1,20) as text(20) ) as whs_filter_code3  ,
cast(substring(a.whs_filter_code4,1,20) as text(20) ) as whs_filter_code4  ,
cast(substring(a.dummy_product_flag,1,10) as text(10) ) as dummy_product_flag  ,
cast(a.shelf_life_days as number(38,0) ) as shelf_life_days  ,
cast(substring(a.pallet_type,1,30) as text(30) ) as pallet_type  ,
cast(substring(a.pallet_config,1,30) as text(30) ) as pallet_config,
cast(a.unique_key as text(255) ) as unique_key
from stg A
LEFT JOIN HIST_ITEM B
on a.item_guid=b.item_guid and a.BUSINESS_UNIT_ADDRESS_GUID=b.BUSINESS_UNIT_ADDRESS_GUID


),


old_dim as (
select
 cast(substring(a.source_system,1,255) as text(255) ) as source_system  ,
cast(a.item_guid as text(255) ) as item_guid  ,
cast(a.item_guid_old as text(255) ) as item_guid_old  ,
cast(a.BUSINESS_UNIT_ADDRESS_GUID as text(255)) as BUSINESS_UNIT_ADDRESS_GUID,
cast(substring(a.source_item_identifier,1,60) as text(60) ) as source_item_identifier  ,
cast(substring(a.source_business_unit_code,1,60) as text(60) ) as source_business_unit_code  ,
cast(substring(a.description,1,60) as text(60) ) as description  ,
cast(substring(a.item_type,1,255) as text(255) ) as item_type  ,
cast(substring(a.branding_code,1,60) as text(60) ) as branding_code  ,
cast(substring(a.branding_desc,1,255) as text(255) ) as branding_desc  ,
cast(a.branding_seq as number(38,0) ) as branding_seq  ,
cast(substring(a.product_class_code,1,60) as text(60) ) as product_class_code  ,
cast(substring(a.product_class_desc,1,255) as text(255) ) as product_class_desc  ,
cast(a.product_class_seq as number(38,0) ) as product_class_seq  ,
cast(substring(a.sub_product_code,1,60) as text(60) ) as sub_product_code  ,
cast(substring(a.sub_product_desc,1,255) as text(255) ) as sub_product_desc  ,
cast(a.sub_product_seq as number(38,0) ) as sub_product_seq  ,
cast(substring(a.strategic_code,1,60) as text(60) ) as strategic_code  ,
cast(substring(a.strategic_desc,1,255) as text(255) ) as strategic_desc  ,
cast(a.strategic_seq as number(38,0) ) as strategic_seq  ,
cast(substring(a.power_brand_code,1,60) as text(60) ) as power_brand_code  ,
cast(substring(a.power_brand_desc,1,255) as text(255) ) as power_brand_desc  ,
cast(a.power_brand_seq as number(38,0) ) as power_brand_seq  ,
cast(substring(a.manufacturing_group_code,1,60) as text(60) ) as manufacturing_group_code  ,
cast(substring(a.manufacturing_group_desc,1,255) as text(255) ) as manufacturing_group_desc  ,
cast(a.manufacturing_group_seq as number(38,0) ) as manufacturing_group_seq  ,
cast(substring(a.pack_size_code,1,60) as text(60) ) as pack_size_code  ,
cast(substring(a.pack_size_desc,1,255) as text(255) ) as pack_size_desc  ,
cast(a.pack_size_seq as number(38,0) ) as pack_size_seq  ,
cast(substring(a.category_code,1,60) as text(60) ) as category_code  ,
cast(substring(a.category_desc,1,255) as text(255) ) as category_desc  ,
cast(a.category_seq as number(38,0) ) as category_seq  ,
cast(substring(a.promo_type_code,1,60) as text(60) ) as promo_type_code  ,
cast(substring(a.promo_type_desc,1,255) as text(255) ) as promo_type_desc  ,
cast(a.promo_type_seq as number(38,0) ) as promo_type_seq  ,
cast(substring(a.sub_category_code,1,60) as text(60) ) as sub_category_code  ,
cast(substring(a.sub_category_desc,1,255) as text(255) ) as sub_category_desc  ,
cast(a.sub_category_seq as number(38,0) ) as sub_category_seq  ,
cast(a.net_weight as number(38,10) ) as net_weight  ,
cast(a.tare_weight as number(38,10) ) as tare_weight  ,
cast(a.avp_weight as number(38,10) ) as avp_weight  ,
cast(substring(a.avp_flag,1,1) as text(1) ) as avp_flag  ,
cast(a.consumer_units_in_trade_units as number(38,0) ) as consumer_units_in_trade_units  ,
cast(a.pallet_qty as number(38,10) ) as pallet_qty  ,
cast(substring(a.current_flag,1,1) as text(1) ) as current_flag  ,
cast(a.gross_weight as number(38,10) ) as gross_weight  ,
cast(a.gross_depth as number(38,10) ) as gross_depth  ,
cast(a.gross_width as number(38,10) ) as gross_width  ,
cast(a.gross_height as number(38,10) ) as gross_height  ,
cast(substring(a.pmp_flag,1,1) as text(1) ) as pmp_flag  ,
cast(a.consumer_units as number(38,0) ) as consumer_units  ,
cast(a.pallet_qty_per_layer as number(38,10) ) as pallet_qty_per_layer  ,
cast(substring(a.item_vat_group,1,60) as text(60) ) as item_vat_group  ,
cast(substring(a.exclude_indicator,1,60) as text(60) ) as exclude_indicator  ,
cast(a.date_inserted as timestamp_ntz(9) ) as date_inserted  ,
cast(a.date_updated as timestamp_ntz(9) ) as date_updated  ,
cast(substring(a.mangrpcd_site,1,255) as text(255) ) as mangrpcd_site  ,
cast(substring(a.mangrpcd_plant,1,255) as text(255) ) as mangrpcd_plant  ,
cast(substring(a.mangrpcd_copack_flag,1,1) as text(1) ) as mangrpcd_copack_flag  ,
cast(substring(a.fin_dim_cost_centre,1,255) as text(255) ) as fin_dim_cost_centre  ,
cast(substring(a.fin_dim_product,1,255) as text(255) ) as fin_dim_product  ,
cast(substring(a.fin_dim_department,1,255) as text(255) ) as fin_dim_department  ,
cast(substring(a.fin_dim_site,1,255) as text(255) ) as fin_dim_site  ,
cast(substring(a.whs_filter_code,1,20) as text(20) ) as whs_filter_code  ,
cast(substring(a.whs_filter_code2,1,20) as text(20) ) as whs_filter_code2  ,
cast(substring(a.whs_filter_code3,1,20) as text(20) ) as whs_filter_code3  ,
cast(substring(a.whs_filter_code4,1,20) as text(20) ) as whs_filter_code4  ,
cast(substring(a.dummy_product_flag,1,10) as text(10) ) as dummy_product_flag  ,
cast(a.shelf_life_days as number(38,0) ) as shelf_life_days  ,
cast(substring(a.pallet_type,1,30) as text(30) ) as pallet_type  ,
cast(substring(a.pallet_config,1,30) as text(30) ) as pallet_config,
cast(a.unique_key as text(255) ) as unique_key
from 
HIST_ITEM A
LEFT join stg B
on a.item_guid=b.item_guid and a.BUSINESS_UNIT_ADDRESS_GUID=b.BUSINESS_UNIT_ADDRESS_GUID
where B.source_system is null

),


Final_Dim 
as 
(
SELECT * FROM new_dim 
UNION 
SELECT * FROM old_dim
) 

 Select * from Final_Dim
