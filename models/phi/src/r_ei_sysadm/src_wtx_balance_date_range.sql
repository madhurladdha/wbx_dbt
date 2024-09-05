

with source as (

    select * from {{ source('FACTS_FOR_COMPARE', 'wtx_balance_date_range') }}

),

renamed as (

    select
        from_date,
        to_date

    from source

)

select * from renamed
