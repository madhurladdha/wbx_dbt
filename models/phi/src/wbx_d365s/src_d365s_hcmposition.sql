with
d365_source as (
    select *
    from {{ source("D365S", "hcmposition") }} where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        positionid as positionid,
        cast(modifieddatetime as TIMESTAMP_NTZ) as modifieddatetime,
        modifiedby,
        cast(createddatetime as TIMESTAMP_NTZ) as createddatetime,
        createdby,
        recversion,
        partition as partition,
        recid as recid
    from d365_source
    --where upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select * from renamed
