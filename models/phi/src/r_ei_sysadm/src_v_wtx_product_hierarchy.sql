

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'v_wtx_product_hierarchy') }}

),

renamed as ( 

    select
        node_level,
        branding_code,
        branding,
        product_code,
        product_class,
        sub_product_code,
        sub_product,
        item_sku,
        item_desc,
        strategic_code,
        strategic_desc,
        power_brand_code,
        power_brand_desc,
        manufacturing_group_code,
        manufacturing_group_desc,
        pack_size_code,
        pack_size_desc,
        category_code,
        category_desc,
        promo_type_code,
        promo_type_desc,
        sub_category_code,
        sub_category_desc,
        branding_seq,
        product_class_seq,
        sub_product_seq,
        strategic_seq,
        power_brand_seq,
        manufacturing_group_seq,
        pack_size_seq,
        category_seq,
        promo_type_seq,
        sub_category_seq,
        net_weight,
        tare_weight,
        avp_weight,
        avp_flag,
        pmp_flag,
        consumer_units_in_trade_units,
        consumer_units,
        pallet_qty,
        current_flag,
        gross_weight,
        gross_depth,
        gross_width,
        gross_height,
        pallet_qty_per_layer,
        exclude_indicator,
        fin_dim_product,
        fin_dim_product_desc,
        filter_code1,
        filter_desc1,
        filter_code2,
        filter_desc2,
        pallet_type,
        pallet_config,
        stock_type,
        stock_desc,
        item_type,
        primary_uom,
        primary_uom_desc,
        obsolete_flag_ax,
        tuc,
        dun,
        ean13,
        source,
        obsolete

    from source

)

select * from renamed

