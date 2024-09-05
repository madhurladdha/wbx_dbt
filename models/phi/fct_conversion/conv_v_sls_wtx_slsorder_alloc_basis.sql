with
    source as (

        select * from {{ source("R_EI_SYSADM", "v_sls_wtx_slsorder_alloc_basis") }}

    ),

    renamed as (

        select
            cast(substring(source_system, 1, 8) as text(8)) as source_system,
            cast(
                substring(ship_source_customer_code, 1, 255) as text(255)
            ) as ship_source_customer_code,
            cast(ship_customer_address_guid as text(255)) as ship_customer_address_guid,
            cast(substring(trade_type_code, 1, 60) as text(60)) as trade_type_code,
            cast(
                substring(source_item_identifier, 1, 255) as text(255)
            ) as source_item_identifier,
            cast(item_guid as text(255)) as item_guid,
            cast(
                substring(product_class_code, 1, 60) as text(60)
            ) as product_class_code,
            cast(substring(market_code, 1, 60) as text(60)) as market_code,
            cast(substring(sub_market_code, 1, 60) as text(60)) as sub_market_code,
            cast(substring(branding_code, 1, 60) as text(60)) as branding_code,
            cast(gl_month as timestamp_ntz(9)) as gl_month,
            cast(tot_ca_kg_quantity as number(31, 2)) as tot_ca_kg_quantity,
            cast(tot_shipped_kg_quantity as number(31, 2)) as tot_shipped_kg_quantity,
            cast(tot_kg_ship_to as number(38, 2)) as tot_kg_ship_to,
            cast(perc_ship_to as number(38, 8)) as perc_ship_to,
            cast(tot_kg_item as number(38, 2)) as tot_kg_item,
            cast(perc_item as number(38, 8)) as perc_item,
            cast(tot_kg_trade_type_item as number(38, 2)) as tot_kg_trade_type_item,
            cast(perc_trade_type_item as number(38, 8)) as perc_trade_type_item,
            cast(tot_kg_month as number(38, 2)) as tot_kg_month,
            cast(perc_month as number(38, 8)) as perc_month,
            cast(tot_kg_trade_type as number(38, 2)) as tot_kg_trade_type,
            cast(perc_trade_type as number(38, 8)) as perc_trade_type,
            cast(tot_kg_product_class as number(38, 2)) as tot_kg_product_class,
            cast(perc_product_class as number(38, 8)) as perc_product_class,
            cast(
                tot_kg_trade_type_product_class as number(38, 2)
            ) as tot_kg_trade_type_product_class,
            cast(
                perc_trade_type_product_class as number(38, 8)
            ) as perc_trade_type_product_class,
            cast(tot_kg_branding_code as number(38, 2)) as tot_kg_branding_code,
            cast(perc_branding_code as number(38, 8)) as perc_branding_code,
            cast(tot_kg_market_code as number(38, 2)) as tot_kg_market_code,
            cast(perc_market_code as number(38, 8)) as perc_market_code,
            cast(tot_kg_submarket as number(38, 2)) as tot_kg_submarket,
            cast(perc_submarket as number(38, 8)) as perc_submarket,
            cast(
                tot_kg_market_code_product_class as number(38, 2)
            ) as tot_kg_market_code_product_class,
            cast(
                perc_market_code_product_class as number(38, 8)
            ) as perc_market_code_product_class,
            cast(
                tot_kg_submarket_product_class as number(38, 2)
            ) as tot_kg_submarket_product_class,
            cast(
                perc_submarket_product_class as number(38, 8)
            ) as perc_submarket_product_class,
            cast(
                tot_kg_trade_type_branding_code as number(38, 2)
            ) as tot_kg_trade_type_branding_code,
            cast(
                perc_trade_type_branding_code as number(38, 8)
            ) as perc_trade_type_branding_code,
            cast(
                tot_kg_market_code_branding_code as number(38, 2)
            ) as tot_kg_market_code_branding_code,
            cast(
                perc_market_code_branding_code as number(38, 8)
            ) as perc_market_code_branding_code,
            cast(
                tot_kg_submarket_branding_code as number(38, 2)
            ) as tot_kg_submarket_branding_code,
            cast(
                perc_submarket_branding_code as number(38, 8)
            ) as perc_submarket_branding_code,
            cast(load_date as date) as load_date,
            cast(update_date as date) as update_date
        -- cast(unique_key as text(255) ) as unique_key
        from source

    )

select *
from renamed
