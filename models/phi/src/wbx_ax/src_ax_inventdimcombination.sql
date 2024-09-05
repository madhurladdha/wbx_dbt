

with source as (

    select * from {{ source('WEETABIX', 'inventdimcombination') }}

),

renamed as (

    select
        itemid,
        inventdimid,
        distinctproductvariant,
        retailvariantid,
        createddatetime,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
