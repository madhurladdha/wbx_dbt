

with source as (

    select * from {{ source('WEETABIX', 'inventtransorigin') }}

),

renamed as (

    select
        inventtransid,
        referencecategory,
        referenceid,
        itemid,
        iteminventdimid,
        party,
        dataareaid,
        recversion,
        partition,
        recid,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby

    from source

)

select * from renamed
