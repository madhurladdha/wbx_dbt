

with source as (

    select * from {{ source('WEETABIX', 'dimensionhierarchy') }}

),

renamed as (

    select
        name,
        description,
        isdraft,
        issystemgenerated,
        structuretype,
        hashkey,
        focusstate,
        deletedversion,
        draftname,
        draftdescription,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
