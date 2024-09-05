

with source as (

    select * from {{ source('WEETABIX', "EXC_Fact_Volume_Cache") }}

),

renamed as (

    select
        day_idx,
        sku_idx,
        cust_idx,
        scen_idx,
        isonpromo_si,
        isonpromo_so,
        ispreorpostpromo_si,
        ispreorpostpromo_so,
        listingactive,
        total_baseretentionpercentage,
        total_si_preorpostdippercentage,
        total_so_preorpostdippercentage,
        vol_stat_base_fc_si,
        vol_stat_base_fc_so,
        vol_override_si,
        vol_override_so,
        vol_effective_base_fc_si,
        vol_effective_base_fc_so,
        vol_promo_total_si,
        vol_promo_total_so,
        vol_cannib_loss_si,
        vol_cannib_loss_so,
        vol_pp_dip_si,
        vol_pp_dip_so,
        vol_total_si,
        vol_total_so,
        vol_si_actual,
        vol_so_actual,
        vol_total_adjust_si,
        vol_total_adjust_so,
        is_vol_total_nonzero

    from source

)

select * from renamed
