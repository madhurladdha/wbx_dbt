with

d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_level_value") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        ordinal,
        display_value as displayvalue,
        dimension_attribute_value as dimensionattributevalue,
        dimension_attribute_value_group as dimensionattributevaluegroup,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed