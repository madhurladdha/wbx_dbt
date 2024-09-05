with

d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_value_set_item") }}
    where _fivetran_deleted = 'FALSE'

),

renamed as (

    select
        'D365' as source,
        dimension_attribute_value_set as dimensionattributevalueset,
        dimension_attribute_value as dimensionattributevalue,
        display_value as displayvalue,
        modifiedby as modifiedby,
        recversion as recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed