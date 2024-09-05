with
d365_source as (
    select *
    from {{ source("D365", "hcm_position_hierarchy") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (

    select
        'D365' as source,
        position as position,
        valid_from as validfrom,
        validfromtzid,
        valid_to as validto,
        validtotzid,
        parent_position as parentposition,
        position_hierarchy_type as positionhierarchytype,
        modifieddatetime,
        modifiedby,
        createddatetime,
        createdby,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed
