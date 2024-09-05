

with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Scenario') }}

),

renamed as (

    select
        promo_idx,
        scen_idx

    from source

)

select * from renamed
