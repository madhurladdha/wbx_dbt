with source as (

    select * from {{ source('WEETABIX', 'wbxcustomerallowedproducts') }}

),

renamed as (

    select
        accountnum,
        fromdate,
        inventdim,
        inventsizeid,
        itemid,
        todate,
        upper(dataareaid) as dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed 