

with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'ctp_plant_wc_day') }}

),

renamed as (

    select
        _line,
        plant,
        work_center_name,
        snapshot_day,
        effective_date,
        expiration_date,
        _fivetran_synced

    from source

)

select * from renamed
