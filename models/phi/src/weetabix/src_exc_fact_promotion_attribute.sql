with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Attribute') }}

),

renamed as (

select
PROMO_IDX,
ATTRIBUTE_IDX
    from source

)

select * from renamed