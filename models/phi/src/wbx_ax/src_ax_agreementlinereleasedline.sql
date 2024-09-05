

with source as (

    select * from {{ source('WEETABIX', 'agreementlinereleasedline') }}

),

renamed as (

    select
        agreementline,
        referencerelationtype,
        isdeleted,
        ismodified,
        custinvoicetrans,
        projinvoiceitem,
        purchlineinventtransid,
        purchlinedataareaid,
        saleslineinventtransid,
        saleslinedataareaid,
        vendinvoicetrans,
        recversion,
        partition,
        recid

    from source

)

select * from renamed
