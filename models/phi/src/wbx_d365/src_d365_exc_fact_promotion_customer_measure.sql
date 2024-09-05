with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Customer_Measure') }}

),

renamed as (

    select

PROMO_IDX,
CUST_IDX,
PROMOMEASURE_IDX,
PROMOMEASURE_VALUE
    from source

)

select * from renamed