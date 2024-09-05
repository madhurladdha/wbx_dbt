

with source as (

    select * from {{ source('WEETABIX', 'inventtransposting') }}

),

renamed as (

    select
        itemid,
        transdate,
        voucher,
        postingtype,
        postingtypeoffset,
        inventtranspostingtype,
        isposted,
        projid,
        inventtransorigin,
        ledgerdimension,
        offsetledgerdimension,
        defaultdimension,
        transbegintime,
        transbegintimetzid,
        dataareaid,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
