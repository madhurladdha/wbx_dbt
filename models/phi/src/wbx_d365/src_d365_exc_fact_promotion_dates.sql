with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Dates') }}

),

renamed as (

    select
PROMO_IDX,
PROMODATE_IDX,
PROMODATE_VALUE
    from source

)

select * from renamed