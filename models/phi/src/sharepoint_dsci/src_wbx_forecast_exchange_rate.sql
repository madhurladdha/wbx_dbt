

with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'wtx_forecast_exchange_rate') }}

),

renamed as (

    select
        _line,
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
        _fivetran_synced

    from source

)

select * from renamed

