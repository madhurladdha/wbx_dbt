with
d365_source as (
    select *
    from {{ source("D365S", "inventmodelgroupitem") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(itemdataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        upper(itemdataareaid) as itemdataareaid,
        modelgroupid as modelgroupid,
        itemid as itemid,
        upper(modelgroupdataareaid) as modelgroupdataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed
