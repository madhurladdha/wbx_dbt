with
d365_source as (
    select *
    from {{ source("D365", "dimension_attribute_value_group_combination") }}
    where _fivetran_deleted = 'FALSE'


),

renamed as (


    select
        'D365' as source,
        ordinal,
        dimension_attribute_value_combination as dimensionattributevaluecombo,
        dimension_attribute_value_group as dimensionattributevaluegroup,
        recversion,
        partition as partition,
        recid as recid
    from d365_source

)

select *
from renamed