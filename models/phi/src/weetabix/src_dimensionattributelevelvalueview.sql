with source as (
    select
        t1.dimensionattribute as "DIMENSIONATTRIBUTE",
        t1.recid as "ATTRIBUTEVALUERECID",
        t1.entityinstance as "ENTITYINSTANCE",
        t1.hashkey as "ATTRIBUTEVALUEHASHKEY",
        t1.partition as "PARTITION",
        1010 as "RECID",
        MAX(t2.recid) as "MAXOFRECID",
        t2.partition as "PARTITION#2",
        t2.displayvalue as "DISPLAYVALUE",
        t3.partition as "PARTITION#3",
        t3.dimensionattributevaluecombo as "VALUECOMBINATIONRECID"
    from {{ ref('src_dimensionattributevalue') }} as t1
    cross join {{ ref('src_dimensionattributelevelvalue') }} as t2
    cross join {{ ref('src_dimensionattrvaluegroupcombo') }} as t3
    where
        (
            t1.recid = t2.dimensionattributevalue
            and (t1.partition = t2.partition)
        )
        and (
            t2.dimensionattributevaluegroup = t3.dimensionattributevaluegroup
            and (t2.partition = t3.partition)
        )
    group by
        t1.dimensionattribute,
        t1.recid,
        t1.entityinstance,
        t1.hashkey,
        t1.partition,
        t2.partition,
        t2.displayvalue,
        t3.partition,
        t3.dimensionattributevaluecombo
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
