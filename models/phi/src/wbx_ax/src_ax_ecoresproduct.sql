

with source as (

    select * from {{ source('WEETABIX', 'ecoresproduct') }}

),

renamed as (

    select
        productmaster,
        retaitotalweight,
        variantconfigurationtechnology,
        retailcolorgroupid,
        retailsizegroupid,
        retailstylegroupid,
        isprodvariantunitconvenabled,
        instancerelationtype,
        displayproductnumber,
        searchname,
        producttype,
        pdscwproduct,
        modifiedby,
        recversion,
        relationtype,
        partition,
        recid,
        notinuse,
        purchstopped,
        salesstopped,
        stockstopped

    from source

)

select * from renamed
