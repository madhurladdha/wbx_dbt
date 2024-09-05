with
source as (
    select * from {{ source("SHAREPOINT_DSCI", "wbx_ibe_budget_sku_pl") }}
),

renamed as (

    select
        _line,
        tratypcde,
        comcde_5_d,
        cyr,
        cyrper,
        budqty,
        waste_red_val,
        cleaning_val,
        value_eng,
        labour_adj_val,
        budkgs,
        budpallets,
        avpkgs,
        kgs_bars,
        gross_value_total,
        edlptotal,
        rsatotal,
        settlement_total,
        gincent_total,
        incentive_forced,
        add_incent_total,
        other_total,
        back_margin_total,
        net_value_total,
        avp_gross_up,
        net_val_gross_up,
        raw_materials_total,
        packaging_total,
        labour_total,
        bought_in_total,
        co_packing_total,
        rye_adj_total,
        total_cost,
        exp_trade_spend,
        exp_consumer_spend,
        pif_isa,
        pif_trade,
        pif_trade_oib,
        pif_trade_red,
        pif_trade_avp,
        pif_trade_enh,
        mif_category,
        mif_customer_marketing,
        mif_field_marketing,
        mif_isa,
        mif_range_support_incentive,
        mif_trade,
        isa_extra,
        product_group,
        frozen_forecast,
        _fivetran_synced

    from source
)

select *
from renamed
