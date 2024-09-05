

with source as (

    select * from {{ source('R_EI_SYSADM', 'v_sls_wtx_slsorder_alloc_basis_trade_type') }}

),

renamed as (

    select
        source_system,
        trade_type_code,
        source_item_identifier,
        item_guid,
        product_class_code,
        market_code,
        sub_market_code,
        branding_code,
        gl_month,
        tot_ca_kg_quantity,
        tot_shipped_kg_quantity,
        tot_kg_item,
        perc_item,
        tot_kg_trade_type_item,
        perc_trade_type_item,
        tot_kg_month,
        perc_month,
        tot_kg_trade_type,
        perc_trade_type,
        tot_kg_product_class,
        perc_product_class,
        tot_kg_trade_type_product_class,
        perc_trade_type_product_class,
        tot_kg_branding_code,
        perc_branding_code,
        tot_kg_market_code,
        perc_market_code,
        tot_kg_submarket,
        perc_submarket,
        tot_kg_market_code_product_class,
        perc_market_code_product_class,
        tot_kg_submarket_product_class,
        perc_submarket_product_class,
        tot_kg_trade_type_branding_code,
        perc_trade_type_branding_code,
        tot_kg_market_code_branding_code,
        perc_market_code_branding_code,
        tot_kg_submarket_branding_code,
        perc_submarket_branding_code,
        load_date,
        update_date

    from source

)

select * from renamed
