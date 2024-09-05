

with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'ibe_history_pl') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        date,
        trade_type,
        sku,
        volume,
        gross_selling_value,
        added_value_pack,
        growth_incentives,
        edlp,
        early_settlement_discount,
        rsa_incentives,
        retro,
        avp_discount,
        off_invoice,
        promo_fixed_funding,
        fixed_annual_payments,
        direct_shopper_marketing,
        other_direct_payments,
        pcos_std_ingredients,
        pcos_std_packaging,
        pcos_std_labour,
        pcos_std_co_packing,
        pcos_std_bought_in,
        pcos_std_other,
        pcos_var_ingredients,
        pcos_var_packaging,
        pcos_var_labour,
        pcos_var_co_packing,
        pcos_var_bought_in,
        pcos_var_other,
        indirect_shopper_marketing,
        category,
        other_indirect_payments,
        field_marketing,
        other_trade,
        marketing_agency_fees,
        research,
        continuous_research,
        market_research,
        sponsorship,
        sales_promotions,
        pack_artwork_design,
        pos_materials,
        samples_issued,
        pr,
        advertising_tv,
        tv_advertising_production,
        press_advertising_consumer,
        press_advertising_production_consumer,
        radio_time,
        radio_time_production,
        website_marketing,
        poster_space,
        poster_production,
        digital_media,
        digital_media_production

    from source

)

select * from renamed

