with source as (

    select * from {{ source('WEETABIX', 'EXC_Dim_Promotion_AttributeGroup') }}

),

renamed as (

    select
ATTRIBUTEGROUP_IDX,
ATTRIBUTEGROUP_CODE,
ATTRIBUTEGROUP_NAME,
ATTRIBUTEGROUP_SORT,
ISVISIBLEONPROMOTION,
ISVISIBLEONTEMPLATE,
ISVISIBLEONPOWEREDITOR,
CONTROLTYPE

    from source

)

select * from renamed