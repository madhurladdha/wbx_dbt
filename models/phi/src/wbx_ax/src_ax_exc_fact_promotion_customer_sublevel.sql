

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Customer_SubLevel') }}

),

renamed as (

    select
        promo_idx,
        sub_cust_idx

    from source

)

select * from renamed
