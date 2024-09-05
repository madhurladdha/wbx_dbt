with
d365_source as (
    select *
    from {{ source("D365", "eco_res_product_variant_dimension_value") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365' as source,
        null as color,
        null as configuration,
        size_ as size_,
        null as style,
        instance_relation_type as instancerelationtype,
        distinct_product_variant as distinctproductvariant,
        product_dimension_attribute as productdimensionattribute,
        retail_weight as retailweight,
        recversion as recversion,
        relationtype as relationtype,
        partition as partition,
        recid as recid

    from d365_source order by source

)

select *
from renamed
