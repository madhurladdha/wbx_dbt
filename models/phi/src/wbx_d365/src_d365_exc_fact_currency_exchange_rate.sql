

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Currency_Exchange_Rate') }}

),

renamed as (

    select
        option_idx,
        valid_from_date,
        valid_to_date,
        valid_from_day_idx,
        valid_to_day_idx,
        value

    from source

)

select * from renamed
