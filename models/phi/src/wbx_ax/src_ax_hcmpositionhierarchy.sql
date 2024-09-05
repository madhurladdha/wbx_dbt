with source as (

    select * from {{ source('WEETABIX', 'hcmpositionhierarchy') }}

),

renamed as (

    select
        position,
        validfrom,
        validfromtzid,
        validto,
        validtotzid,
        parentposition,
        positionhierarchytype,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed