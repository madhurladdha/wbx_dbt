with source as (

    select * from {{ source('WEETABIX', 'hcmposition') }}

),

renamed as (

    select
        positionid,
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