with source as (

    select * from {{ source('WEETABIX', 'hcmpositionworkerassignment') }}

),

renamed as (

    select
        position,
        validfrom,
        validfromtzid,
        validto,
        validtotzid,
        assignmentreasoncode,
        worker,
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