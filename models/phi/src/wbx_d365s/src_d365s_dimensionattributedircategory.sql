with
d365_source as (
    select *
    from {{ source("D365S", "dimensionattributedircategory") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (


    select
        'D365S' as source,
        dimensionattribute as dimensionattribute,
        dircategory as dircategory,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select * from renamed
