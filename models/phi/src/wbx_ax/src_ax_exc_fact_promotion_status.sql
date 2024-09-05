with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Status') }}

),

renamed as (

    select
PROMO_IDX,
STATUS_IDX
    from source

)

select * from renamed