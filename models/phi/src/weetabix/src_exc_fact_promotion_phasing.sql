with source as (

    select * from {{ source('WEETABIX', 'EXC_Fact_Promotion_Phasing') }}

),

renamed as (

    select
PROMO_IDX,
PHASE_IDX,
PHASE_EFFECT_IDX
    from source

)

select * from renamed