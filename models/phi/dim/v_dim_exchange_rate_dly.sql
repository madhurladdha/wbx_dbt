{{
    config(
        materialized="view",
        tags="rdm_core"
    )
}}
/*this view is created in order to supplement the exchange rate Model with certain scenarios(conversion rates) that are not already present,
basically when from and to currency codes are same */
with
    dim_exch_rate as (
        select curr_from_code, curr_to_code, curr_conv_rt, eff_from_d
        from {{ ref("src_currency_exch_rate_dly_dim") }}
    ),

    same_currency as (
        select distinct
            curr_from_code,
            curr_from_code as curr_to_code,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as curr_conv_rt,
            '1900-01-01' as eff_from_d
        from {{ ref("src_currency_exch_rate_dly_dim") }}
        union
        select distinct
            curr_to_code as curr_from_code,
            curr_to_code,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as curr_conv_rt,
            '1900-01-01' as eff_from_d
        from {{ ref("src_currency_exch_rate_dly_dim") }}
    ),

    final as (
        select *
        from dim_exch_rate
        union
        select *
        from same_currency
    )

select *
from final
