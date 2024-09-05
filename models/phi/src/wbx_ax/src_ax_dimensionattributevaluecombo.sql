

with source as (

    select * from {{ source('WEETABIX', 'dimensionattributevaluecombo') }}

),

renamed as (

    select
        displayvalue,
        mainaccount,
        accountstructure,
        ledgerdimensiontype,
        hash,
        modifiedby,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
