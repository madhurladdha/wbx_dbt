

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Mechanic') }}

),

renamed as (

    select
PROMO_IDX,
MECHANIC_IDX
    from source

)

select * from renamed
