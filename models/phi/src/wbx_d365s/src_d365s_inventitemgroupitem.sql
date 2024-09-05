with
d365_source as (
    select *
    from {{ source("D365S", "inventitemgroupitem") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(
            trim(itemdataareaid)
        ) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (

    select
        'D365S' as source,
        itemid as itemid,
        upper(itemdataareaid) as itemdataareaid,
        itemgroupid as itemgroupid,
        upper(itemgroupdataareaid) as itemgroupdataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed