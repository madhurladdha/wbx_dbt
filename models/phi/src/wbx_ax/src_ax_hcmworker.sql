

with source as (

    select * from {{ source('WEETABIX', 'hcmworker') }}

),

renamed as (

    select
        person,
        personnelnumber,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        recversion,
        partition,
        recid,
        wbxsnowdroppersonnelnumber

    from source

)

select * from renamed
