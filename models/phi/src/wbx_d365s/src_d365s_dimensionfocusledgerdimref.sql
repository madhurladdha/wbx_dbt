with
d365_source as (
    select *
    from {{ source("D365S", "dimensionfocusledgerdimensionreference") }}
    where _fivetran_deleted = 'FALSE'
),



renamed as (

    select
        'D365S' as source,
        focusledgerdimension as focusledgerdimension,
        accountentryledgerdimension as accountentryledgerdimension,
        focusdimensionhierarchy as focusdimensionhierarchy,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
