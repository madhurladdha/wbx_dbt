with
d365_source as (
    select *
    from {{ source("D365", "eco_res_product") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (


    select
        'D365' as source,
        productmaster as productmaster,
        retaitotalweight as retaitotalweight,
        variantconfigurationtechnology as variantconfigurationtechnology,
        null as retailcolorgroupid,
        null as retailsizegroupid,
        null as retailstylegroupid,
        isproductvariantunitconversionenabled as isprodvariantunitconvenabled,
        instance_relation_type as instancerelationtype,
        display_product_number as displayproductnumber,
        search_name as searchname,
        product_type as producttype,
        pds_cwproduct as pdscwproduct,
        modifiedby as modifiedby,
        recversion as recversion,
        relationtype as relationtype,
        partition as partition,
        recid as recid,
        null as notinuse,
        null as purchstopped,
        null as salesstopped,
        null as stockstopped
    from d365_source

)

select *
from renamed
