with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'weetabix_forecast_snapshot_dimension') }}

),

renamed as (

    select
        _line,
        snapshot_date ,
        snapshot_model,
        snapshot_code,
        snapshot_type,
        snapshot_desc,
         _fivetran_synced
    from source

)

select * from renamed
