with source as (

    select * from {{ source('WEETABIX', 'inventtransoriginprodtable') }}

),

renamed as (

    select
        prodorderid,
        inventtransorigin,
        prodorderdataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed