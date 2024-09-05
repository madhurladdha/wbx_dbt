with
d365_source as (
    select *
    from {{ source("D365", "dimension_hierarchy") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        name as name,
        description as description,
        is_draft as isdraft,
        is_system_generated as issystemgenerated,
        structure_type as structuretype,
        hash_key as hashkey,
        focus_state as focusstate,
        deleted_version as deletedversion,
        draft_name as draftname,
        draft_description as draftdescription,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
