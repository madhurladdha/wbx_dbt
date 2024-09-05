

with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_Mechanic_L1') }}

),

renamed as (

    select
MECHANIC_L1_IDX,
MECHANIC_L1_CODE,
MECHANIC_L1_NAME
    from source

)

select * from renamed
