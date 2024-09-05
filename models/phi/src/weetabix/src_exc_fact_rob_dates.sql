

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_ROB_Dates') }}

),

renamed as (

    select
        rob_idx,
        date_start,
        date_end,
        daysinrob,
        date_start_idx,
        date_end_idx,
        end_early_date,
        end_early_day_idx

    from source

)

select * from renamed
