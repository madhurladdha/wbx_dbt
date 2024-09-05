with
d365_source as (
    select *
    from {{ source("D365S", "inventtransposting") }}
    where
        _fivetran_deleted = 'FALSE'
        and upper(dataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}
),

renamed as (


    select
        'D365S' as source,
        itemid as itemid,
        cast(transdate as TIMESTAMP_NTZ) as transdate,
        voucher as voucher,
        postingtype as postingtype,
        postingtypeoffset as postingtypeoffset,
        inventtranspostingtype as inventtranspostingtype,
        isposted as isposted,
        null as projid,
        inventtransorigin as inventtransorigin,
        ledgerdimension as ledgerdimension,
        offsetledgerdimension as offsetledgerdimension,
        defaultdimension as defaultdimension,
        cast(transbegintime as TIMESTAMP_NTZ) as transbegintime,
        null as transbegintimetzid,
        upper(dataareaid) as dataareaid,
        recversion as recversion,
        partition as partition,
        recid as recid

    from d365_source

)

select * from renamed


