  {{
    config(
    materialized = env_var('DBT_MAT_TABLE'),
    tag=['ax_hist_dim']
    )
}}

WITH old_dim AS 
(
    SELECT * FROM {{source('WBX_PROD','dim_wbx_item_ext')}} where  {{env_var("DBT_PICK_FROM_CONV")}}='Y' /*adding variable to include/exclude conversion model data.if variable DBT_PICK_FROM_CONV has value 'Y' then conversion model will pull data from hist else it will be null */
),

old_plant as
 (
    select * from {{ref('conv_dim_wbx_plant_dc')}}
 ),


conv_dim as 
(
    Select
    item_guid,
    a.business_unit_address_guid as business_unit_address_guid_old,
    b.plantdc_address_guid_new  as business_unit_address_guid,
    a.source_system as source_system,
    null as item_guid_old,
    source_item_identifier,
    a.source_business_unit_code as source_business_unit_code_old,
    b.source_business_unit_code_new as source_business_unit_code,
    description,
        item_type,
        branding_code,
        branding_desc,
        branding_seq,
        product_class_code,
        product_class_desc,
        product_class_seq,
        sub_product_code,
        sub_product_desc,
        sub_product_seq,
        strategic_code,
        strategic_desc,
        strategic_seq,
        power_brand_code,
        power_brand_desc,
        power_brand_seq,
        manufacturing_group_code,
        manufacturing_group_desc,
        manufacturing_group_seq,
        pack_size_code,
        pack_size_desc,
        pack_size_seq,
        category_code,
        category_desc,
        category_seq,
        promo_type_code,
        promo_type_desc,
        promo_type_seq,
        sub_category_code,
        sub_category_desc,
        sub_category_seq,
        net_weight,
        tare_weight,
        avp_weight,
        avp_flag,
        consumer_units_in_trade_units,
        pallet_qty,
        current_flag,
        gross_weight,
        gross_depth,
        gross_width,
        gross_height,
        pmp_flag,
        consumer_units,
        pallet_qty_per_layer,
        item_vat_group,
        exclude_indicator,
        a.date_inserted,
        a.date_updated,
        mangrpcd_site,
        mangrpcd_plant,
        mangrpcd_copack_flag,
        fin_dim_cost_centre,
        fin_dim_product,
        fin_dim_department,
        fin_dim_site,
        whs_filter_code,
        whs_filter_code2,
        whs_filter_code3,
        whs_filter_code4,
        dummy_product_flag,
        shelf_life_days,
        pallet_type,
        pallet_config
    from old_dim a
    left join old_plant b
    on  a.source_system = b.source_system 
    and a.business_unit_address_guid = b.plantdc_address_guid
)

select {{ dbt_utils.surrogate_key(['ITEM_GUID','BUSINESS_UNIT_ADDRESS_GUID']) }} AS UNIQUE_KEY,* from conv_dim qualify ROW_NUMBER() over(PARTITION by unique_key order by 1)=1