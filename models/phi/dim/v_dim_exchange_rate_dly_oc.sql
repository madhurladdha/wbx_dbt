{{
    config(
        materialized="view",
        tags="rdm_core"
    )
}}
/*this view is created in order to supplement the exchange rate Model with certain scenarios(conversion rates) that are not already present in UOM,
basically when from tand to currency codes are same */
with
    dim_exch_rate as (
        select source_system,curr_from_code, curr_to_code, curr_conv_rt, eff_from_d
        from {{ ref("dim_wbx_exchange_rate_dly_oc") }}
        /* 29-May-2023: change from from src_currency_exch_rate_dly_dim_oc to dim_wbx_exchange_rate_dly_oc */
    ),

    same_currency as (
        select distinct
            source_system,curr_from_code,
            curr_from_code as curr_to_code,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as curr_conv_rt,
            '1900-01-01' as eff_from_d
        from {{ ref("dim_wbx_exchange_rate_dly_oc") }}
        union
        select distinct
            source_system,curr_to_code as curr_from_code,
            curr_to_code,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as curr_conv_rt,
            '1900-01-01' as eff_from_d
        from {{ ref("dim_wbx_exchange_rate_dly_oc") }}
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
	