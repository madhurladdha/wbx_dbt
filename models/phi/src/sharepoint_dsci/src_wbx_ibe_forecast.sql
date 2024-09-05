

with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'weetabix_ibe_forecast_in') }}

),

renamed as (

    select
        _line,
        site,
        rsatotal,
        back_margin_total,
        pif_trade_avp,
        calendar_year,
        gross_amount,
        mif_customer_marketing,
        total_cost,
        edlptotal,
        rye_adj_total,
        forecast_ca_qty,
        packaging_total,
        add_incent_total,
        settlement_total,
        incentive_forced,
        labour_total,
        planning_week_end_date,
        raw_materials_total,
        other_total,
        fixed_annual_payment,
        avp_gross_up,
        promo_off_invoice_bonus,
        co_packing_total,
        gincent_total,
        bought_in_total,
        item_number,
        promo_fixed_funding,
        calendar_month_no,
        pif_trade_enh,
        trade_type,
        consumer_marketing,
        _fivetran_synced,
        rsatotal_new

    from source

)

select * from renamed