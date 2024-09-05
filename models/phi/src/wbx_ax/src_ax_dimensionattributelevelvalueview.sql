
with source as (

    select * from {{ source('WEETABIX', 'dimensionattributelevelvalueview') }}

),

renamed as (

    select
        dimensionattribute,
        attributevaluerecid,
        entityinstance,
        attributevaluehashkey,
        partition,
        recid,
        maxofrecid,
        "PARTITION#2",
        displayvalue,
        "PARTITION#3",
        valuecombinationrecid

    from source

)

select * from renamed
