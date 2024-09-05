

with source as (

    select * from {{ source('WEETABIX', 'ecoresproductvariantdimvalue') }}

),

renamed as (

    select
        color,
        configuration,
        size_,
        style,
        instancerelationtype,
        distinctproductvariant,
        productdimensionattribute,
        retailweight,
        recversion,
        relationtype,
        partition,
        recid

    from source

)

select * from renamed
