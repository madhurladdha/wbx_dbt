with
d365_source as (
    select *
    from {{ source("D365S", "ecoresproductvariantdimensionvalue") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        null as color,
        null as configuration,
        /*size_ and relationtype set to null
        columns not found in wbx_d365s*/
        null as size_,
        null as style,
        instancerelationtype as instancerelationtype,
        distinctproductvariant as distinctproductvariant,
        productdimensionattribute as productdimensionattribute,
        retailweight as retailweight,
        recversion as recversion,
        null as relationtype,
        partition as partition,
        recid as recid

    from d365_source order by source

)

select *
from renamed
