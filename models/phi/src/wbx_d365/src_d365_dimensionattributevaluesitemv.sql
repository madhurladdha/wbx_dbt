

with source as (

    select * from {{ source('WEETABIX', 'dimensionattributevaluesitemv') }}

),

renamed as (

    select
        dimensionattributevalue,
        dimensionattributevalueset,
        displayvalue,
        setitemrecid,
        partition,
        recid,
        "PARTITION#2",
        dimensionattribute,
        entityinstance,
        attributevaluerecid

    from source

)

select * from renamed
