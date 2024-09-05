

with source as (

    select * from {{ source('R_EI_SYSADM', 'prc_wtx_forecast_stg') }}

),

renamed as (

    select
        source_item_identifier,
        description,
        company_code,
        year,
        scenario,
        oct_volume,
        nov_volume,
        dec_volume,
        jan_volume,
        feb_volume,
        mar_volume,
        apr_volume,
        may_volume,
        jun_volume,
        jul_volume,
        aug_volume,
        sep_volume,
        oct_cost_eur,
        nov_cost_eur,
        dec_cost_eur,
        jan_cost_eur,
        feb_cost_eur,
        mar_cost_eur,
        apr_cost_eur,
        may_cost_eur,
        jun_cost_eur,
        jul_cost_eur,
        aug_cost_eur,
        sep_cost_eur,
        oct_cost_base,
        nov_cost_base,
        dec_cost_base,
        jan_cost_base,
        feb_cost_base,
        mar_cost_base,
        apr_cost_base,
        may_cost_base,
        jun_cost_base,
        jul_cost_base,
        aug_cost_base,
        sep_cost_base,
        load_date

    from source

)

select * from renamed
