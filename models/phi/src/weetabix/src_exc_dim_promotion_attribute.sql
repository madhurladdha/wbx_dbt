with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_Attribute') }}

),

renamed as (

    select
ATTRIBUTE_IDX,
ATTRIBUTE_CODE,
ATTRIBUTE_NAME,
ATTRIBUTEGROUP_IDX,
ATTRIBUTE_SORT,
ISPROMOTIONSDEFAULTFORGROUP,
ISTEMPLATESDEFAULTFORGROUP,
ISUSEDINCOPY
    from source

)

select * from renamed