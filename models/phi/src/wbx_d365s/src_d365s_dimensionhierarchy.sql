with
d365_source as (
    select *
    from {{ source("D365S", "dimensionhierarchy") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365S' as source,
        name as name,
        description as description,
        isdraft as isdraft,
        issystemgenerated as issystemgenerated,
        structuretype as structuretype,
        hashkey as hashkey,
        focusstate as focusstate,
        deletedversion as deletedversion,
        draftname as draftname,
        draftdescription as draftdescription,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
