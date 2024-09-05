with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Currency') }}

),

renamed as (

    select
CURRENCY_IDX,
CURRENCY_CODE,
CURRENCY_NAME,
CURRENCY_SYMBOL
    from source

)

select * from renamed