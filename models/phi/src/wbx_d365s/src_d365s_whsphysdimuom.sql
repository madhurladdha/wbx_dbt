with d365_source as (
    select *
    from {{ source("D365S", "whsphysdimuom") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

),

renamed as (
    select
        'D365S' as source,
        uom,
        itemid as itemid,
        NULL as physdimid,
        depth,
        height,
        weight,
        width,
        modifieddatetime,
        modifiedby,
        upper(dataareaid) as dataareaid,
        recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed