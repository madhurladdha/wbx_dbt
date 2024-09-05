/*  29-May-2023: repointing the main source from EI_RDM.currency_exch_rate_mth_dim to DIM_ENT.dim_exchange_rate_mth
*/

with source as (

    select * from {{ source('DIM_ENT', 'dim_exchange_rate_mth') }}

),

renamed as (

    select
        exch_rate_type,
        from_currency_code,
        to_currency_code,
        fiscal_year_period_no,
        effective_date_id,
        effective_date,
        expiration_date,
        effective_fiscal_period,
        expire_fiscal_period,
        curr_conversion_rt,
        curr_inversion_rt,
        load_date

    from source

)

select * from renamed