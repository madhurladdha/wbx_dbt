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
        select
            exch_rate_type,
            from_currency_code,
            to_currency_code,
            fiscal_year_period_no,
            curr_conversion_rt
        from {{ ref("src_currency_exch_rate_mth_dim") }}
    ),

    same_currency as (
        select distinct
            'dummy' as exch_rate_type,
            from_currency_code,
            from_currency_code as to_currency_code,
            '190001' as fiscal_year_period_no,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as curr_conversion_rt
        from {{ ref("src_currency_exch_rate_mth_dim") }}
        union
        select distinct
            'dummy' as exch_rate_type,
            to_currency_code as from_currency_code,
            to_currency_code,
            '190001' as fiscal_year_period_no,
            {{ lkp_constants("DEFAULT_CONVERSION_RATE") }} as curr_conversion_rt
        from {{ ref("src_currency_exch_rate_mth_dim") }}
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
