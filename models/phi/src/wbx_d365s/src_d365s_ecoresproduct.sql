with
d365source as (
    select *
    from {{ source("D365S", "ecoresproduct") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365S' as source,
        /*productmaster, retaitotalweight
        variantconfigurationtechnology, relationtype
        set to null columns not found in WBX_D365S*/
        null as productmaster,
        null as retaitotalweight,
        null as variantconfigurationtechnology,
        null as retailcolorgroupid,
        null as retailsizegroupid,
        null as retailstylegroupid,
        null as isprodvariantunitconvenabled,
        instancerelationtype as instancerelationtype,
        displayproductnumber as displayproductnumber,
        searchname as searchname,
        producttype as producttype,
        pdscwproduct as pdscwproduct,
        modifiedby as modifiedby,
        recversion as recversion,
        null as relationtype,
        partition as partition,
        recid as recid,
        null as notinuse,
        null as purchstopped,
        null as salesstopped,
        null as stockstopped
    from d365source

)

select *
from renamed
