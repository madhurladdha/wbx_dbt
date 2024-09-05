

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_ROB_Status') }}

),

renamed as (

    select
        rob_idx,
        status_idx

    from source

)

select * from renamed
