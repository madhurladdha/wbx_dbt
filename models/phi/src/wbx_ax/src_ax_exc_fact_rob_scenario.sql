

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_ROB_Scenario') }}

),

renamed as (

    select
        rob_idx,
        scen_idx

    from source

)

select * from renamed
