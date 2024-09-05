

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_PC_Customer') }}

),

renamed as (

    select
        idx,
        parent_idx,
        custlevel_idx,
        created_ts

    from source

)

select * from renamed
