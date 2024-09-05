with
d365_source as (

    select *
    from {{ source("D365S", "agreementline") }}
    where _fivetran_deleted = 'FALSE'
),

child_source as (

    select *
    from {{ source("D365S", "agreementlinequantitycommitment") }}
    where _fivetran_deleted = 'FALSE'
),

renamed as (
    select
        'D365S' as source,
        null as commitedamount,
        child_source.commitedquantity as commitedquantity,
        child_source.productunitofmeasure as productunitofmeasure,
        child_source.priceunit as priceunit,
        child_source.priceperunit as priceperunit,
        child_source.linediscountamount as linediscountamount,
        child_source.pdscwcommitedquantity as pdscwcommitedquantity,
        d365_source.instancerelationtype as instancerelationtype,
        d365_source.linenumber as linenumber,
        d365_source.agreementlinetype as agreementlinetype,
        d365_source.agreementlineproduct as agreementlineproduct,
        cast(d365_source.expirationdate as TIMESTAMP_NTZ) as expirationdate,
        cast(d365_source.effectivedate as TIMESTAMP_NTZ) as effectivedate,
        d365_source.linediscountpercent as linediscountpercent,
        d365_source.agreedreleaselineminamount as agreedreleaselineminamount,
        d365_source.agreedreleaselinemaxamount as agreedreleaselinemaxamount,
        d365_source.ispriceinformationmandatory as ispriceinformationmandatory,
        d365_source.ismaxenforced as ismaxenforced,
        d365_source.isdeleted as isdeleted,
        d365_source.ismodified as ismodified,
        d365_source.category as category,
        d365_source.itemid as itemid,
        d365_source.agreement as agreement,
        upper(d365_source.itemdataareaid) as itemdataareaid,
        d365_source.inventdimid as inventdimid,
        upper(d365_source.inventdimdataareaid) as inventdimdataareaid,
        null as projectprojid,
        null as projectdataareaid,
        d365_source.defaultdimension as defaultdimension,
        d365_source.currency as currency,
        d365_source.recversion as recversion,
        null as relationtype,
        d365_source.partition as partition,
        d365_source.recid as recid
    from d365_source
    left join child_source on d365_source.recid = child_source.recid
    where upper(itemdataareaid) in {{ env_var("DBT_D365_COMPANY_FILTER") }}

)

select * from renamed
