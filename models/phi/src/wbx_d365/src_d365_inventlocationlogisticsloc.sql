with
d365_source as (
    select *
    from {{ source("D365", "invent_location_logistics_location") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        invent_location as inventlocation,
        location as location,
        null as attentiontoaddressline,
        is_default as isdefault,
        is_primary as isprimary,
        is_postal_address as ispostaladdress,
        is_private as isprivate,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select *
from renamed
