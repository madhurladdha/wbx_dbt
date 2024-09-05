with
d365_source as (
    select *
    from {{ source("D365S", "hcmpositionhierarchy") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        position as position,
        cast(validfrom as TIMESTAMP_NTZ) as validfrom,
        null as validfromtzid,
        cast(validto as TIMESTAMP_NTZ) as validto,
        null as validtotzid,
        parentposition as parentposition,
        positionhierarchytype as positionhierarchytype,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
