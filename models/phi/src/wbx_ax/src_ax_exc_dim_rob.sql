

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_ROB') }}

),

renamed as (

    select
        rob_idx,
        rob_code,
        rob_name,
        apptype_idx,
        rob_author_user_idx,
        date_created,
        file_location,
        last_saved_by_user_idx,
        integration_code

    from source

)

select * from renamed
