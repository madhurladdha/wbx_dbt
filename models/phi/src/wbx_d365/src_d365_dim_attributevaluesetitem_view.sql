with source as (
    select
        t1.dimensionattributevalue as "DIMENSIONATTRIBUTEVALUE",
        t1.dimensionattributevalueset as "DIMENSIONATTRIBUTEVALUESET",
        t1.displayvalue as "DISPLAYVALUE",
        t1.recid as "SETITEMRECID",
        t1.partition as "PARTITION",
        t1.recid as "RECID",
        t2.partition as "PARTITION#2",
        t2.dimensionattribute as "DIMENSIONATTRIBUTE",
        t2.entityinstance as "ENTITYINSTANCE",
        t2.recid as "ATTRIBUTEVALUERECID"
    from {{ ref('src_dimensionattributevaluesetitem') }} as t1
    cross join {{ ref('src_dimensionattributevalue') }} as t2
    where (
        t1.dimensionattributevalue = t2.recid
        and (t1.partition = t2.partition)
    )
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