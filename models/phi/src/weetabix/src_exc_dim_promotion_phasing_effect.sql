with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_Phasing_Effect') }}

),

renamed as (

    select
PHASE_EFFECT_IDX,
PHASE_EFFECT_CODE,
PHASE_EFFECT_NAME,
PHASE_EFFECT_COLOUR,
PHASE_TYPE_IDX
    from source

)

select * from renamed