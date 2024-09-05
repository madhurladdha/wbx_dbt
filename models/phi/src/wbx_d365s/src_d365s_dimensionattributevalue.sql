with

d365_source as (
    select *
    from {{ source("D365S", "dimensionattributevalue") }}
    where _fivetran_deleted = 'FALSE'


),

renamed as (


    select
        'D365S' as source,
        dimensionattribute as dimensionattribute,
        issuspended as issuspended,
        cast(activefrom as TIMESTAMP_NTZ) as activefrom,
        cast(activeto as TIMESTAMP_NTZ) as activeto,
        istotal as istotal,
        entityinstance as entityinstance,
        isblockedformanualentry as isblockedformanualentry,
        null as groupdimension,
        hashkey as hashkey,
        isdeleted as isdeleted,
        owner as owner,
        isbalancing_psn as isbalancing_psn,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed