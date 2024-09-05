with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_Dates') }}

),

renamed as (

    select
PROMODATE_IDX,
PROMODATE_CODE,
PROMODATE_NAME,
PROMODATE_TYPE,
PROMODATEGROUP_IDX
    from source

)

select * from renamed