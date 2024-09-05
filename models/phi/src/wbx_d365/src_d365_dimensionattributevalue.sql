with

d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_value") }}
    where _fivetran_deleted = 'FALSE'


),

renamed as (


    select
        'D365' as source,
        dimension_attribute as dimensionattribute,
        is_suspended as issuspended,
        active_from as activefrom,
        active_to as activeto,
        is_total as istotal,
        entity_instance as entityinstance,
        is_blocked_for_manual_entry as isblockedformanualentry,
        null as groupdimension,
        hash_key as hashkey,
        is_deleted as isdeleted,
        owner as owner,
        is_balancing_psn as isbalancing_psn,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed