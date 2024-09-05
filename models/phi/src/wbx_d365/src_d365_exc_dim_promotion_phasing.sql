with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_Phasing') }}

),

renamed as (

    select
PHASE_IDX,
PHASE_CODE,
INTEGRATION_CODE,
PHASE_NAME,
PHASE_TYPE_IDX,
PHASE_EFFECT_IDX,
PHASE_LENGTH,
ISCUSTOMPHASING,
CUSTLEVEL_IDX,
PRODLEVEL_IDX,
AUTHOR_USER_IDX	
    from source

)

select * from renamed