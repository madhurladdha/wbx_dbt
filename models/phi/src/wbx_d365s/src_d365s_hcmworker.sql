with
d365_source as (
    select *
    from {{ source("D365S", "hcmworker") }} where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        person as person,
        personnelnumber as personnelnumber,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby as modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby as createdby,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxsnowdroppersonnelnumber
    from d365_source

)

select * from renamed
