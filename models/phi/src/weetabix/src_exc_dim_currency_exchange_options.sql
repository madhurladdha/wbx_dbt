

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Currency_Exchange_Options') }}

),

renamed as (

    select
        option_idx,
        option_code,
        option_name,
        currency_from_idx,
        currency_to_idx

    from source

)

select * from renamed
