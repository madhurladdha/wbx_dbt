

with source as (

    select * from {{ source('EI_RDM', 'prc_wtx_forecast_exch_rate_stg') }}

),

renamed as (

    select
        year,
        scenario,
        oct,
        nov,
        dec,
        jan,
        feb,
        mar,
        apr,
        may,
        jun,
        jul,
        aug,
        sep,
        load_date

    from source

)

select * from renamed
