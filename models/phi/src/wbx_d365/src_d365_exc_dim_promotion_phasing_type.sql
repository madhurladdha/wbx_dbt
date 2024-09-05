with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_Phasing_Type') }}

),

renamed as (

    select
PHASE_TYPE_IDX,
PHASE_TYPE_CODE,
PHASE_TYPE_NAME,
PHASE_TYPE_UNIT_LABEL,
PHASE_TYPE_COLOUR,
DATE_CREATED
    from source

)

select * from renamed