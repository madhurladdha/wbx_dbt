

with source as (

    select * from {{ source('WEETABIX', 'agreementline') }}

),

renamed as (

    select
        commitedamount,
        commitedquantity,
        productunitofmeasure,
        priceunit,
        priceperunit,
        linediscountamount,
        pdscwcommitedquantity,
        instancerelationtype,
        linenumber,
        agreementlinetype,
        agreementlineproduct,
        expirationdate,
        effectivedate,
        linediscountpercent,
        agreedreleaselineminamount,
        agreedreleaselinemaxamount,
        ispriceinformationmandatory,
        ismaxenforced,
        isdeleted,
        ismodified,
        category,
        itemid,
        agreement,
        itemdataareaid,
        inventdimid,
        inventdimdataareaid,
        projectprojid,
        projectdataareaid,
        defaultdimension,
        currency,
        recversion,
        relationtype,
        partition,
        recid

    from source

)

select * from renamed
