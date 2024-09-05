

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotions_Group_Promotion') }}

),

renamed as (

    select

PROMO_GROUP_IDX,
PROMO_IDX
    from source

)

select * from renamed
