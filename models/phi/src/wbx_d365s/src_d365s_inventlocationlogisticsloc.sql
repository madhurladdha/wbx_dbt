with
d365_source as (
    select *
    from {{ source("D365S", "inventlocationlogisticslocation") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        inventlocation as inventlocation,
        location as location,
        null as attentiontoaddressline,
        isdefault as isdefault,
        isprimary as isprimary,
        ispostaladdress as ispostaladdress,
        isprivate as isprivate,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
