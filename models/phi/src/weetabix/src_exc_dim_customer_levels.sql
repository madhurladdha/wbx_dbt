

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Customer_Levels') }}

),

renamed as (

    select
        custlevel_idx,
        custlevel_code,
        custlevel_name,
        hierarchy_idx

    from source

)

select * from renamed
