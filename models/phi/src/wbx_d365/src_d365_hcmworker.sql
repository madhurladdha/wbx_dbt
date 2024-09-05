with
d365_source as (
    select *
    from {{ source("D365", "hcm_worker") }} where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365' as source,
        person as person,
        personnel_number as personnelnumber,
        modifieddatetime as modifieddatetime,
        modifiedby as modifiedby,
        createddatetime as createddatetime,
        createdby as createdby,
        recversion as recversion,
        partition as partition,
        recid as recid,
        null as wbxsnowdroppersonnelnumber
    from d365_source

)

select * from renamed
