with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Customers') }}

),

renamed as (

    select
PROMO_IDX,
CUST_IDX
    from source

)

select * from renamed