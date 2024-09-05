

with source as (

    select * from {{ source('WEETABIX', 'wbxcusttableext') }}

),

renamed as (

    select
        anainvoicelocation,
        anaorderlocation,
        asnrequired,
        contractorindicator,
        custaccount,
        deliveryinstruction,
        ediinvoice,
        fullpallet,
        oneproductperpallet,
        shelflife,
        shelflifecheckneeded,
        poddelayindays,
        dataareaid,
        recversion,
        partition,
        recid,
        podrequired,
        leadtime,
        minimumorderquantitydefault,
        minimumorderquantitypallets,
        custleadtime,
        totalsoqtydisc,
        additionaldisc

    from source

)

select * from renamed
