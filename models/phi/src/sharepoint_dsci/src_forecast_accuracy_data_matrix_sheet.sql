with source as (

    select * from {{ source('SHAREPOINT_DSCI', 'forecast_accuracy_data_matrix_sheet_1') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        month_2,
        month,
        month_3,
        month_1_

    from source

)

select * from renamed